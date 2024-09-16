import numpy as np
from sklearn.metrics import accuracy_score
from joblib import dump
from typing import Dict
import xgboost as xgb
from sklearn.model_selection import GridSearchCV


MODEL_PATH = '../models/'

def evaluation(model: xgb.XGBRegressor, X_test: np.ndarray, y_test: np.ndarray) -> Dict[str, float]:
    y_pred_test = model.predict(X_test)
    return {
        'accuracy': accuracy_score(y_test, y_pred_test)
    }


def model_train(model: xgb.XGBRegressor, X_train: np.ndarray,
           X_test: np.ndarray, y_train: np.ndarray, y_test: np.ndarray) -> Dict[str, float]:
    
    model = xgb.XGBClassifier(objective='binary:logistic', eval_metric='logloss')
    model.fit(X_train, y_train)

    dump(model, MODEL_PATH + 'Logistic_Regression.joblib')


    # dump(model, MODEL_PATH + 'XGBoost_model.joblib')
    param_grid = {
        'max_depth': [3, 4, 5, 7, 8],
        'learning_rate': [0.01, 0.1, 0.2, 0.3, 0.4],
        'n_estimators': [50, 100, 200, 300, 400, 500]
    }

    # Create a GridSearchCV object
    grid_search = GridSearchCV(estimator=xgb.XGBClassifier(
            objective='binary:logistic', eval_metric='logloss'),
            param_grid=param_grid,
            scoring='accuracy',
            cv=3,
            verbose=1
        )

    # Fit the grid search to the data
    grid_search.fit(X_train, y_train)

    # Print the best parameters
    print("Best parameters found: ", grid_search.best_params_)
    
    dump(grid_search, MODEL_PATH + 'XGBoost_classifier.joblib')

    return {'model performance before tuning': evaluation(grid_search, X_test, y_test)}

