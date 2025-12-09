import xgboost as xgb
from sklearn.metrics import roc_auc_score

class XGBSignalModel:
    def __init__(self, params=None):
        self.params = params or {
            "objective": "binary:logistic",
            "max_depth": 4,
            "eta": 0.03,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "eval_metric": "auc"
        }
        self.model = None

    def train(self, X, y):
        dtrain = xgb.DMatrix(X, label=y)
        self.model = xgb.train(self.params, dtrain, num_boost_round=300)

    def predict_proba(self, X):
        dtest = xgb.DMatrix(X)
        return self.model.predict(dtest)
