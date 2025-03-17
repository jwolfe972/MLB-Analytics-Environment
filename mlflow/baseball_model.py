import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LinearRegression, RidgeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier, ExtraTreeClassifier

from sklearn.model_selection import train_test_split
from sklearn.manifold import TSNE
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, accuracy_score, f1_score, recall_score, precision_score
from sklearn.preprocessing import MinMaxScaler
import mlflow
from mlflow.models import infer_signature
from sklearn.neural_network import MLPClassifier
from sklearn.metrics.pairwise import rbf_kernel
import seaborn as sns
import matplotlib.pyplot as plt
#import baseball_dataset
#TODO: use MLP NN model
#TODO: Bring in Airflow for downloading the dataset and train dataset on it to see if they are there


#Ensure the dataset is completely up to date
#baseball_dataset.stats_per_year(2024)


minmax_scaler = MinMaxScaler( feature_range=(0,1))

# Load and preprocess data
data = pd.read_csv('stats.csv')
data = data.loc[:, ~data.columns.str.contains('^Unnamed')]
data['Runs/Game'] = data['Total Runs']/data['Total Games']
data = data.drop_duplicates(keep='first')



#data['L10'] = data['Win?'].rolling(window=10, min_periods=1).sum()

#Differentiate between hitting statistics and pitching statistics
hitting_stats = ['AVG', 'OBP', 'SLG', 'WRC+', 'WAR', 'K Percentage', 'BB Percentage', 'BSR', 'AVG/5 Players', 'OBP/5 Players', 'SLG/5 Players', 'WAR/5 Players', 'WRC+/5 Players', 'K Percentage/5 Players', 'BB Percentage/5 Players', 'AVG/Week', 'OBP/Week', 'SLG/Week', 'WAR/Week', 'WRC+/Week', 'K Percentage/Week', 'BB Percentage/Week', 'Runs/Game']
pitching_stats = ['Opposing K/9', 'Opposing HR/9', 'Opposing BB/9', 'ERA', 'Opposing War', 'Opposing K/9/5 Players', 'Opposing BB/9/5 Players', 'ERA/5 Players', 'Opposing WAR/5 Players', 'Opposing K/9/Week', 'Opposing BB/9/Week', 'ERA/Week', 'Opposing WAR/Week']
#Separate between X and y datasets
features = hitting_stats + pitching_stats


X_normalized = minmax_scaler.fit_transform(data[features]) 
Y = data['Win?']


X_df = pd.DataFrame(X_normalized, columns=features)
X_df['Win?'] = Y
# train = data.sample(frac=0.8, random_state=42)
# test = data.drop(train.index)

X_train, X_test, Y_train, Y_test = train_test_split(data[features], Y, test_size=.2)






# X_train = train.drop(['Runs Scored', 'Win?', 'Date', 'Offensive Team', 'Defensive Team', 'Total Games', 'Total Runs', 'RBIs'], axis=1)
# X_train = X_train[features]

# X_test = test.drop(['Runs Scored', 'Win?', 'Date', 'Offensive Team', 'Defensive Team', 'Total Games', 'Total Runs', 'RBIs'], axis=1)
# X_test = X_test[features]
# y_train = train['Win?']
# y_test = test['Win?']

# X_train = minmax_scaler.fit_transform(X_train)
# X_test = minmax_scaler.fit_transform(X_test)

#Initialize and train the model to predict team wins


mlflow.set_tracking_uri('http://127.0.0.1:5000')
mlflow.set_experiment('MLB Win Prediction Experiment')

with mlflow.start_run():
    params = {
        'max_depth': 4,
        'n_estimators': 101,
        'learning_rate': .5
    }

    plt.figure(figsize=(30, 30))
    sns.heatmap(X_df.corr(), annot=True,cmap="crest")
    plt.savefig('corr_plot.png')
    mlflow.log_artifact('corr_plot.png')


    win_model = RidgeClassifier()
    win_model.fit(X_train, Y_train)
    predictions = win_model.predict(X_test)


    accuracy = accuracy_score(Y_test, predictions)
    f1 = f1_score(Y_test, predictions)
    recall = recall_score(Y_test, predictions)
    precision = precision_score(Y_test, predictions)
    mlflow.log_params(win_model.get_params())
    mlflow.log_metric('Accuracy', accuracy)
    mlflow.log_metric('F1', f1)
    mlflow.log_metric('Recall', recall)
    mlflow.log_metric('Precision', precision)

    mlflow.set_tag('Training Data Run', 'MLP Classifier')

    signature = infer_signature(X_train, win_model.predict(X_train))

    model_info = mlflow.sklearn.log_model(
        sk_model=win_model,
        artifact_path="win_model",
        signature=signature,
        input_example=X_train,
        registered_model_name="Gradient-Boost-Classifier"
    )

    

    






#Download the win model
# with open('win_model.pkl', 'wb') as file:
#     pickle.dump(win_model, file)

# #Initialize and train the model to predict team runs
# run_model = LinearRegression()
# y_train = train['Runs Scored']
# y_test = test['Runs Scored']
# run_model.fit(X_train, y_train)
# predictions = run_model.predict(X_test)

# #Download the run model
# with open('run_model.pkl', 'wb') as file:
#     pickle.dump(run_model, file)

# print(mean_absolute_error(y_test, predictions))
# print(mean_squared_error(y_test, predictions))
# print(r2_score(y_test, predictions))

