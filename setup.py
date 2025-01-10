class Weights_Allocation:
    
    def __init__(self, data):
        self.data = data 
        self.prepare_data()
    
    def prepare_data(self):
        """
        Extracts and preprocesses returns data for portfolio construction.
        """
        self.returns_data = self.data.filter(like='returns_')
        self.asset_names = [col.split('_')[1] for col in self.returns_data.columns]
    
        imputer = SimpleImputer(strategy='mean')
        self.returns_data_imputed = pd.DataFrame(imputer.fit_transform(self.returns_data), columns=self.returns_data.columns)

    def train_gradient_boosting(self):
        """
        Train a Gradient Boosting model to predict portfolio returns based on asset returns.
        """
        X = self.returns_data_imputed
        y = np.mean(self.returns_data_imputed.values, axis=1) 
        self.model = GradientBoostingRegressor(n_estimators=100, random_state=42)
        self.model.fit(X, y)
    
    def predict_portfolio_returns(self):
        """
        Predict the portfolio returns using the trained Gradient Boosting model.
        """
        if not hasattr(self, 'model'):
            raise ValueError("Model has not been trained yet. Call train_gradient_boosting first.")
        
        X = self.returns_data_imputed
        predicted_returns = self.model.predict(X)
        return predicted_returns
    
    def portfolio_statistics(self, weights):
        """
        Calculates portfolio mean return and volatility for given weights.
        """
        predicted_returns = self.predict_portfolio_returns()
        
        portfolio_returns = np.dot(self.returns_data_imputed.values, weights)  
        mean_return = np.mean(portfolio_returns)
        volatility = np.std(portfolio_returns)
        return mean_return, volatility
    
    def objective_function(self, weights):
        """
        Objective function to minimize (negative Sharpe ratio).
        """
        mean_return, volatility = self.portfolio_statistics(weights)
        sharpe_ratio = mean_return / volatility
        return -sharpe_ratio  
    
    def optimize_weights(self):
        """
        Finds optimal weights that maximize the Sharpe ratio using the Gradient Boosting model.
        """
        num_assets = len(self.asset_names)
        initial_weights = np.ones(num_assets) / num_assets
        bounds = [(0, 1) for _ in range(num_assets)]  
        constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1}]  

        result = minimize(
            self.objective_function,
            initial_weights,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        self.optimal_weights = result.x
        return self.optimal_weights
    
    def build_portfolio(self, use_optimal_weights=True):
        """
        Builds portfolio returns based on the provided or optimized weights.
        """
        if use_optimal_weights:
            self.optimize_weights()
            self.weights = self.optimal_weights
        elif self.weights is None:
            raise ValueError("Weights must be provided if not using optimization.")
        
        predicted_returns = self.predict_portfolio_returns()
        portfolio_returns = np.dot(self.returns_data_imputed.values, self.weights)
        return portfolio_returns
