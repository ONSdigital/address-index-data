"""
ONS Address Index - Optimise the Probabilistic Parser
=====================================================

A simple script to run random search over CRF parameters to find an optimised model.
Uses a smaller training data set to speed up the process. Three-fold cross-validation
is being used to assess the performance. Uses weighted F1-score as the metrics to
maximise.


Requirements
------------

:requires: scikit-learn
:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)
:requires: scipy
:requires: matplotlib


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.3
:date: 24-Oct-2016
"""
import ProbabilisticParser.common.tokens as t
from scipy import stats
import sklearn_crfsuite
from sklearn_crfsuite import metrics
from sklearn.metrics import make_scorer
from sklearn.model_selection import RandomizedSearchCV
import matplotlib.pyplot as plt
import pickle


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


def plotSearchSpace(rs, param1='c1', param2='c2', outpath='/Users/saminiemi/Projects/ONS/AddressIndex/figs/'):
    """
    Generates a figure showing the search results as a function of two parameters.

    :param rs: scikit-learn randomised search object
    :ttype rs: object
    :param param1: name of the first parameter that was used in the optimisation
    :type param1: str
    :param param2: name of the second parameter that was used in the optimisation
    :type param2: str
    :param outpath: location to which the figure will be stored
    :type outpath: str

    :return: None
    """
    _x = [s.parameters[param1] for s in rs.grid_scores_]
    _y = [s.parameters[param2] for s in rs.grid_scores_]
    _c = [s.mean_validation_score for s in rs.grid_scores_]
    # print(rs.cv_results_)

    plt.figure()
    ax = plt.gca()
    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_xlabel(param1)
    ax.set_ylabel(param2)
    ax.set_title("Randomised Hyperparameter Search CV Results (min={:0.3}, max={:0.3})".format(min(_c), max(_c)))
    sc = ax.scatter(_x, _y, c=_c, s=60, alpha=0.7, edgecolors=[0, 0, 0])
    plt.colorbar(sc)
    plt.tight_layout()
    plt.savefig(outpath + 'hyperparameterOptimisation.pdf')
    plt.close()


def optimiseModel(X_train, y_train, X_test, y_test):
    """
    Randomised search to optimise the regularisation and other parameters of the CRF model.
    The regularisation parameters are drawn from exponential distributions.

    :param X_train: training data in 2D array
    :param y_train: training data labels
    :param X_test: holdout data in 2D array
    :type y_test: holdout data true labels

    :return: None
    """
    # define fixed parameters and parameters to search
    crf = sklearn_crfsuite.CRF(algorithm='lbfgs', verbose=False)

    # search parameters random draws from exponential functions and boolean for transitions
    params_space = {'c1': stats.expon(scale=0.5), 'c2': stats.expon(scale=0.05),
                    'all_possible_transitions': [True, False]}

    # metrics needs a list of labels
    # labels = t.LABELS
    labels = ['OrganisationName', 'SubBuildingName', 'BuildingName', 'BuildingNumber', 'StreetName',
              'Locality', 'TownName', 'Postcode']

    # use (flattened) f1-score for evaluation
    # todo: should one use complete sequence rather than f1?
    f1_scorer = make_scorer(metrics.flat_f1_score, average='weighted', labels=labels)

    print('Performing randomised search using cross-validations...')
    rs = RandomizedSearchCV(crf, params_space,
                            cv=3,
                            verbose=1,
                            n_jobs=-1,
                            n_iter=50,
                            scoring=f1_scorer)
    rs.fit(X_train, y_train)

    print('saving the optimisation results to a pickled file...')
    fh = open(t.MODEL_PATH + 'optimisation.pickle', mode='wb')
    pickle.dump(rs, fh)
    fh.close()

    crf = rs.best_estimator_
    print('best params:', rs.best_params_)
    print('best CV score:', rs.best_score_)
    print('model size: {:0.2f}M'.format(rs.best_estimator_.size_ / 1000000))

    print('\nHoldout performance:')
    y_pred = crf.predict(X_test)
    sorted_labels = sorted(labels, key=lambda name: (name[1:], name[0]))
    print(metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=3))

    print('Generating a figure...')
    plotSearchSpace(rs)


if __name__ == '__main__':
    X_train, y_train, X_test, y_test = readData()
    optimiseModel(X_train, y_train, X_test, y_test)
