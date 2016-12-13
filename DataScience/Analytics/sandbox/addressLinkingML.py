#!/usr/bin/env python
"""
ONS Address Index - Address Linking
===================================

This script can be used to test which string distance metrics are more important for
solving identifying correct matches.


Running
-------

After all requirements are satisfied, the script can be invoked using CPython interpreter::

    python addressLinkingML.py


Requirements
------------

:requires: pandas (0.19.1)
:requires: numpy (1.11.2)
:requires: scikit-learn (0.18.1)
:requires: matplotlib (1.5.3)


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 12-Dec-2016
"""
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import (RandomForestClassifier, ExtraTreesClassifier)
from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve, auc
import matplotlib
from sklearn.externals import joblib
matplotlib.use('Agg')  # to prevent Tkinter crashing on cdhut-d03
import matplotlib.pyplot as plt


def load_data(filepath='/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/training_data.csv',
              verbose=False):
    """
    Reads in string distance metrics that can be used to train a supervised classification model.
    Returns the data in a dataframe with features and target.

    :param filepath: location and filename to read in
    :type filepath: str
    :param verbose: whether or not to show additional information
    :type verbose: bool

    :return: data containing string distance metrics and target (match=1, non-match=0)
    :rtype: pandas.DataFrame
    """
    columns = {'TestData_Index': np.int64, 'flat_dl': np.float64, 'pao_dl': np.float64, 'building_name_dl': np.float64,
               'building_number_dl': np.float64, 'pao_number_dl': np.float64, 'AddressBase_Index': np.int64,
               'street_dl': np.float64, 'town_dl': np.float64, 'locality_dl': np.float64,  'pao_suffix_dl': np.float64,
               'flatw_dl': np.float64, 'sao_number_dl': np.float64, 'organisation_dl': np.float64,
               'department_dl': np.float64, 'street_desc_dl': np.float64, 'similarity_sum': np.float64,
               'block_mode': np.int32, 'UPRN_old': np.float64, 'UPRN': np.float64}

    data = pd.read_csv(filepath, dtype=columns, low_memory=False, na_values=None, usecols=columns.keys())

    msk = data['UPRN'].isnull() | data['UPRN_old'].isnull()
    data = data.loc[~msk]

    examples = len(data.index)
    print('Found {} observations'.format(examples))

    data = data.fillna(value=0., axis=1)

    msk = data['UPRN_old'] == data['UPRN']
    data['target'] = 0
    data.loc[msk, 'target'] = 1

    data.drop(['UPRN', 'UPRN_old', 'block_mode'], axis=1, inplace=True)

    positives = data['target'].sum()
    negatives = examples - positives

    if verbose:
        print(data.info())

    print('Found {} positives'.format(positives))
    print('Found {} negatives'.format(negatives))

    return data


def check_performance(y_test, y_pred, td_test, output='Logistic'):
    """
    Calculate AUC and plot ROC.

    :param y_test: actual results
    :param y_pred: predicted probabilities for the positive class

    :return: None
    """
    combined_data = pd.DataFrame({'target': y_test, 'probability': y_pred, 'TestData_Index': td_test})

    combined_data.sort_values(by='probability', ascending=False, inplace=True)
    combined_data.drop_duplicates('TestData_Index', keep='first', inplace=True)
    print('Correctly Predicted = {} addresses'.format(combined_data['target'].sum()))

    print('AUC={}'.format(roc_auc_score(y_test, y_pred)))

    fpr, tpr, thresholds = roc_curve(y_test, y_pred)
    roc_auc = auc(fpr, tpr)
    plt.plot(fpr, tpr, lw=2, color='b', label='AUC = %0.2f' % roc_auc)
    plt.plot([0, 1], [0, 1], linestyle='--', lw=1.5, color='k', label='Random')
    plt.xlim([-0.01, 1.01])
    plt.ylim([-0.01, 1.01])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic example')
    plt.legend(loc="lower right")
    plt.savefig('/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/' + output + 'ROC.png')
    plt.close()


def build_model(data):
    """
    Train a simple logistic regression model on 70 per cent of the data and test the performance on 30
    per cent of the data.

    The logistic regression uses L2-regularisation with intercept being fitted. The function outputs
    coefficient weights which can be interpreted as the importance of features. Computes the probabilities
    manually and asserts that they are the same as returned by scikit-learn. This is simply to confirm
    the mechanics of computing probabilities from scikit-learn intercept and coefficients.

    :param data: input data with features and target
    :type data: pandas.DataFrame

    :return: None
    """
    y = data['target'].values
    similarity_sum = data['similarity_sum'].values
    similarity_sum /= similarity_sum.max()

    TestData_Index = data['TestData_Index']

    tmp = data.drop(['target', 'similarity_sum', 'TestData_Index', 'AddressBase_Index'], axis=1)
    columns = np.asarray([x.replace('_dl', '').replace('_', ' ') for x in tmp.columns.values])
    X = tmp.values

    X_train, X_test, y_train, y_test, ss_train, ss_test, td_train, td_test = \
        train_test_split(X, y, similarity_sum, TestData_Index, test_size=0.3, random_state=42)

    print('{} matches in test data'.format(np.sum(y_test)))

    print('similarity sum:')
    check_performance(y_test, ss_test, td_test, output='SimilaritySum')

    lg = LogisticRegression(class_weight='balanced', max_iter=100000, solver='sag', verbose=True, n_jobs=-1)
    rf = RandomForestClassifier(n_estimators=1000, n_jobs=-1, verbose=True)
    et = ExtraTreesClassifier(n_estimators=1000, n_jobs=-1, verbose=True)

    for clf, name in zip((lg, rf, et), ('LogisticRegression', 'RandomForest', 'ExtraTrees')):
        print('\n', name)
        # build model and store on disk
        clf.fit(X_train, y_train)
        joblib.dump(clf, '/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/' + name + '.pkl')

        # predict probabilities and check performance
        y_pred = clf.predict_proba(X_test)
        check_performance(y_test, y_pred[:, 1], td_test, output=name)

        if 'Logistic' in name:
            print('\nFeature Importance:')
            print('Intercept = ', clf.intercept_[0])
            for column, coefficient in zip(columns, clf.coef_[0]):
                print('{0} = {1}'.format(column, coefficient))

            n_features_generator = range(len(clf.coef_[0]))
            indices = np.argsort(clf.coef_[0])[::-1]

            plt.figure(figsize=(16, 12))
            plt.title("Feature Importance")
            plt.bar(n_features_generator, clf.coef_[0][indices], color="r", align="center")
            plt.xticks(n_features_generator, columns, rotation=45)
            plt.xlim([-1, X.shape[1]])
            plt.savefig('/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/' + name + 'FeatureImportance.png')
            plt.tight_layout()
            plt.close()

            manual_probs = 1. / (1 + np.exp(-(clf.intercept_[0] + np.sum(clf.coef_[0] * X_test, axis=1))))
            np.testing.assert_almost_equal(y_pred[:, 1], manual_probs)
        else:
            importances = clf.feature_importances_
            std = np.std([tree.feature_importances_ for tree in clf.estimators_], axis=0)
            indices = np.argsort(importances)[::-1]

            columns = columns[indices]
            n_features_generator = range(X.shape[1])

            # Print the feature ranking
            print("Feature ranking:")
            for column, feature in zip(columns, n_features_generator):
                print("%d. feature %s = %.5f" % (feature + 1, column, importances[indices[feature]]))

            # Plot the feature importances of the forest
            plt.figure(figsize=(16, 12))
            plt.title("Feature Importance")
            plt.bar(n_features_generator, importances[indices],  color="r", yerr=std[indices], align="center")
            plt.xticks(n_features_generator, columns, rotation=45)
            plt.xlim([-1, X.shape[1]])
            plt.savefig('/Users/saminiemi/Projects/ONS/AddressIndex/linkedData/' + name + 'FeatureImportance.png')
            plt.tight_layout()
            plt.close()


if __name__ == "__main__":
    data = load_data()
    build_model(data)
