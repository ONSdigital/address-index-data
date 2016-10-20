"""
ONS Address Index - Optimise the Probabilistic Parser
=====================================================

A simple script to run random search over CRF parameters to find an optimised model.
Uses a smaller training data set to speed up the process.


Requirements
------------

:requires: scikit-learn
:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)
:requires: scipy
:requires: numpy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 20-Oct-2016
"""
import ProbabilisticParser.common.tokens as t
from scipy import stats
import sklearn_crfsuite
import numpy as np
from sklearn_crfsuite import metrics
from sklearn.metrics import make_scorer
from sklearn.model_selection import RandomizedSearchCV


def readData(trainingfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/training100000.xml',
             holdoutfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml',
             verbose=True):
    """
    Read in the training and holdout data from XML files.

    :param trainingfile: location of the training data
    :type trainingfile: str
    :param holdoutfile: location of the holdout data
    :type holdoutfile: str
    :param verbose: whether or not to print to stdout
    :type verbose: bool

    :return: training data and labels, holdout data and labels
    :rtype: list
    """
    if verbose:
        print('Read in training data...')
    X_train, y_train = t.readData(trainingfile)

    if verbose:
        print('Read in holdout data')
    X_test, y_test = t.readData(holdoutfile)

    return X_train, y_train, X_test, y_test


def optimiseModel(X_train, y_train, X_test, y_test):
    """
    Randomised search to optimise the regularisation and other parameters of the CRF model.

    :param X_train:
    :param y_train:
    :param X_test:
    :param y_test:

    :return:
    """
    # define fixed parameters and parameters to search
    crf = sklearn_crfsuite.CRF(algorithm='lbfgs', verbose=False)

    # search parameters random draws from exponential functions
    params_space = {'c1': stats.expon(scale=0.5), 'c2': stats.expon(scale=0.05),
                    'all_possible_transitions': [True, False]}

    # metrics needs a list of labels
    labels = t.LABELS
    # labels = ['OrganisationName', 'SubBuildingName', 'BuildingName', 'BuildingNumber', 'StreetName',
    #           'Locality', 'TownName', 'Postcode']
    print('Labels:', labels)

    # use the same metric for evaluation
    f1_scorer = make_scorer(metrics.flat_f1_score, average='weighted', labels=labels)

    print('Performing randomised search using cross-validations...')
    rs = RandomizedSearchCV(crf, params_space,
                            cv=5,
                            verbose=1,
                            n_jobs=-1,
                            n_iter=100,
                            scoring=f1_scorer)
    rs.fit(X_train, y_train)

    crf = rs.best_estimator_
    print('best params:', rs.best_params_)
    print('best CV score:', rs.best_score_)
    print('model size: {:0.2f}M'.format(rs.best_estimator_.size_ / 1000000))

    print('\nHoldout performance:')
    y_pred = crf.predict(X_test)
    sorted_labels = sorted(labels, key=lambda name: (name[1:], name[0]))
    print(metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=3))

    return crf


if __name__ == '__main__':
    X_train, y_train, X_test, y_test = readData()
    optimiseModel(X_train, y_train, X_test, y_test)
