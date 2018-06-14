# -*- coding: utf-8 -*-
"""
Created May 2018

Use machine learning to combine important features in matching
addresses to create one confidence score

Part 1: Data wrangling for input into machine learning algorithm
Part 2: Machine learning

Want to predict a match (TRUE / FALSE) using:
- Hopper scores?
- debug codes (might need to split these up and specify that they're categorical)
- normalized elastic search score 

@author: ivyONS
"""

###############################################################################
# Import libraries
import pandas as pd
import numpy as np
import time
# using sklearn 0.19.1: # module cross_validation is depricated, use model_selection instead
from sklearn.model_selection import cross_val_predict
#from scipy.stats import sem
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.preprocessing import  StandardScaler , OneHotEncoder, LabelEncoder
from sklearn.base import TransformerMixin, BaseEstimator
from sklearn.feature_selection import SelectKBest, RFECV
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier, export_graphviz
from sklearn.metrics import f1_score, accuracy_score, log_loss, roc_auc_score


###############################################################################
# Define variables to create slim model (drop most)
# split variables by type
debug_var = ['buildingScoreDebug', 'localityScoreDebug', 'unitScoreDebug']
numerical_var = ['h_power', 'e_sigmoid', 'e_score', 'parsing_score', 'structuralScore', 'unitScore']
                           
X_slim_var = numerical_var + debug_var


##############################################################################
# Read in data - combine datasets
tdata_path = '//tdata8/AddressIndex/Beta_Results/'
folder_name = 'May_15_test_threshold0'
datasets = ['EdgeCases', 'CQC', 'PatientRecords','WelshGov']

X_full = pd.DataFrame() 
y_full = pd.Series()
for data_name in datasets:
    print(data_name)
    edge = pd.read_csv(tdata_path + data_name + '/' + folder_name + '/' +data_name +'_ML_features_5_parse.csv', encoding='latin-1')
    edge = edge.assign(dataset=pd.Series([data_name]*edge.shape[0]))
    X_full = X_full.append(edge[['dataset','ID_original','UPRN_comparison','ADDRESS', 'matchedFormattedAddress','UPRN_beta', 'matches']+ X_slim_var],  ignore_index = True )
    y_full = y_full.append(edge['matches']==True)
 
    
###############################################################################    
# feature manipulation - classes that will help us to define the preprocessing in the pipeline     
class fixTypesTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass  
    def transform(self,  X, **transform_params):
        X['unitScore'] = X['unitScore'].apply(abs).astype(np.number)    
        cat_df = X[debug_var].apply(LabelEncoder().fit_transform).apply(lambda x: x.astype('category'))
        num_df = X[numerical_var].astype(np.number)
        res = pd.concat([num_df, cat_df], axis=1)
        return res
    def fit(self, X, y=None, **fit_params):
        return self

class TypeSelector(BaseEstimator, TransformerMixin):
    def __init__(self, dtype):
        self.dtype = dtype
    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        return X.select_dtypes(include=[self.dtype])         
    def fit(self, X, y=None):
        return self    
        
class IdentityTransformer(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass    
    def fit(self, X, y=None):
        return self    
    def transform(self, X):
        return X    
  
    
###############################################################################
# first preprocessing - feature formatting
cat_weight = 0.1
get_features = Pipeline([
    ('customFixTypes', fixTypesTransformer()),
    ('features', FeatureUnion(n_jobs=1, 
        transformer_list=[        
        ('numericals', Pipeline([
            ('selector', TypeSelector(np.number)),
            #('scaler', StandardScaler()),  # all the scores are within 0 and 1 already
        ])),          
        #('boolean', Pipeline([
        #    ('selector', TypeSelector('bool')),
        #])),  # there are no boolean variables in the slim model
        ('categoricals', Pipeline([
            ('selector', TypeSelector('category')),
            ('encoder', OneHotEncoder(n_values = 'auto', handle_unknown='ignore', sparse=False)),  
        ])),  
    ],
    transformer_weights={
            'numericals': 1.0,
            'categoricals': cat_weight,
        })),  
]) 

# apply formatting + get feature names    
X_feat = get_features.fit_transform(X_full)    
X_feat_names = numerical_var + [ (var + '_' + str(y)) for var in debug_var for y in np.sort(X_full[var].unique()) ] 

# drop debug codes with less than 500 cases
X_feat_sum = X_feat.sum(axis=0)
X_feat_keep = X_feat_sum > cat_weight * 500
X_feat_keep[0:len(numerical_var)] = True

X_feat_sub_names = [nam for ind, nam in enumerate(X_feat_names) if X_feat_keep[ind]]
X_feat_sub = X_feat[:,X_feat_keep]

# choose subset cca 100 000 cases
np.random.seed(4321)
sub =  np.logical_or(np.random.choice([True, False], X_feat.shape[0] , p=[0.08, 0.92]), X_full['dataset']=='EdgeCases')
X = X_feat_sub[sub, :]
y =  [i for ind, i in enumerate(y_full) if sub[ind]]


###############################################################################
# define possible methods for feature selection and classification
###############feature_selection
select_features =  [('PCA',  PCA(n_components = 5)),                    
                    ('kbest', SelectKBest(k = 5)),
                    ('none', IdentityTransformer())]
                    #('RFECV_svm', RFECV( SVC(kernel='linear'))),  # it takes ages!
                    #('RFECV_logreg', RFECV( LogisticRegression(penalty='l2')))  ]
                          
################## try out simple models
select_model = [ ('logreg', LogisticRegression(penalty='l2')),
                ('svm', SVC(probability = True)),
               ( 'forest',  RandomForestClassifier(min_samples_leaf=500, max_depth = 5))
           ]  
  
 
###############################################################################
# model selection: calculate couple of metrics for each model + feature subsets  (on subset data)
################## display cross validation F1, accu, logloss, auc     
for model in select_model:
    for selector in select_features:
        print(time.strftime("%H:%M:%S"))
        pipeline = Pipeline([('selector', selector[1]), ('model', model[1])])
        cv_predicted = cross_val_predict(pipeline, X, y, cv=3)                        
        accu = accuracy_score(y, cv_predicted )  
        ff1 = f1_score(y, cv_predicted )  
        cv_predicted = cross_val_predict(pipeline, X, y, cv=3, method='predict_proba') 
        lloss = log_loss(y, cv_predicted )  
        auc = roc_auc_score(y, [x[1] for x in cv_predicted] )  
        print(selector[0], model[0], ' F1:', ff1, ' Accu:', accu, 'LogLoss:', lloss, 'AUC:', auc) 

################## results summary:
# feature_selection: looks like PCA is making it worse
# model_selection: logreg or random forest are best. F1 > .98, Accu > .992, logloss < 0.03  
# so we will export the predicted probabilities and calculate our usual matching performance in R 


###############################################################################
# explore some of the models, e.g. what are the important features? (on all data) 
###############################################################################
#### visualise decision tree
clf = DecisionTreeClassifier(min_samples_leaf=5000, max_depth = 5).fit(X_feat_sub, y_full)
export_graphviz(clf, feature_names=X_feat_sub_names, out_file='//ts003/spakui$/My documents/python/slim_tree.dot')     
#### print kbest features
top_k = 5
kbest = SelectKBest(k = top_k).fit(X_feat_sub, y_full)
feat_df = pd.DataFrame(data = {'name': X_feat_sub_names, 'score':  kbest.scores_,# 'p_val': kbest.pvalues_, 
                                'top5_kbest' : kbest.get_support()})
#### top most important coeficients in logistic regression
logreg = LogisticRegression(penalty='l2') .fit(X_feat_sub,y_full)
feat_df['reg_coef'] = logreg.coef_[0]
dummy = abs(logreg.coef_[0])
dummy.sort()
feat_df['top5_reg'] = abs(logreg.coef_[0]) >= dummy[-top_k]
#### top most important features in random forest
rfc = RandomForestClassifier(min_samples_leaf=5000, max_depth = 5).fit(X_feat_sub, y_full)
feat_df['rfc_importance'] = rfc.feature_importances_
dummy = rfc.feature_importances_
dummy.sort()
feat_df['top5_rfc'] =  feat_df['rfc_importance'] >= dummy[-top_k]
feat_df['prod'] = abs(feat_df['reg_coef'])*feat_df['score']*feat_df['rfc_importance']
feat_df['top5_any'] = feat_df['top5_reg'] | feat_df['top5_kbest'] | feat_df['top5_rfc']

print(feat_df.sort_values(by = ['top5_any','prod'], ascending=False, inplace =False).iloc[0:20,[0,2,4,6,7])

#### Recursive feature elimination with cross-validation but it takes looong time (so using only subset data)
rfecv_reg = RFECV( LogisticRegression(penalty='l2'), step=1, cv=3, scoring='neg_log_loss')
rfecv_reg = rfecv_reg.fit(X, y) 
feat_df['top_rfecv_reg'] = rfecv_reg.support_

rfecv_rfc = RFECV( RandomForestClassifier(min_samples_leaf=500, max_depth = 5), step=1, cv=3, scoring='neg_log_loss')
rfecv_rfc = rfecv_rfc.fit(X, y) 
feat_df['top_rfecv_rfc'] = (rfecv_rfc.ranking_) <= top_k

print(feat_df.sort_values(by = ['top_rfecv_rfc','top_rfecv_reg', 'prod'], ascending=False, inplace =False).iloc[0:50,])
 
################## results summary:
# e_sigmoid is the best feature by far
# h_power and some debug codes pop up inconsistently  
# parsing score is irrelevant in all models

top_features = ['e_sigmoid','h_power','e_score', 'structuralScore',
                'buildingScoreDebug_91', 'buildingScoreDebug_96']
top_features = [0, 1, 2, 4, 25, 27]

#############################################################################
# export predictions for simple models
#############################################################################
# train on the small subset and fit to all data   

rfc = RandomForestClassifier(min_samples_leaf=500, max_depth = 6).fit(X[:, top_features], y)
logreg = LogisticRegression(penalty='l2').fit(X[:, top_features], y)

rfc_predicted = rfc.predict_proba(X_feat_sub[:,top_features])    
logreg_predicted = logreg.predict_proba(X_feat_sub[:,top_features])  

X_full = X_full.assign(rfc_predicted = [x[1] for x in rfc_predicted])
X_full = X_full.assign(logreg_predicted = [x[1] for x in logreg_predicted])
    

for data_name in datasets:
    print(data_name)
    filename = tdata_path + data_name + '/' + folder_name + '/' +data_name +'_ML_slim_predicted.csv'
    edge =  X_full[(X_full.dataset == data_name)]
    edge.to_csv(filename, index=True)


###########################################################################
#
##########################################################################