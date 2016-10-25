"""
ONS Address Index - Test the Performance of the Probabilistic Parser
====================================================================

A simple script to test the performance of a trained probabilistic parser
using holdout data. Computes the number of tokens that were correctly identified.
In addition, computes the fraction of complete addresses correctly parsed and
the performance metric per token type.


Requirements
------------

:requires: sklearn-crfsuite (http://sklearn-crfsuite.readthedocs.io/en/latest/index.html)
:requires: seaborn
:requires: matplotlib
:requires: numpy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.4
:date: 20-Oct-2016
"""
from ProbabilisticParser import parser
import ProbabilisticParser.common.tokens as t
import sklearn_crfsuite
from sklearn_crfsuite import metrics
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


# set seaborn style
sns.set_style("whitegrid")
sns.set_context("poster")
sns.set(rc={"figure.figsize": (12, 12)})
sns.set(font_scale=1.5)


def predict(address):
    """
    Predict the tokens using a trained probabilistic parser model.
    The address parser must have been trained before parse can be called.

    :param address: raw address string to be parsed
    :type address: str

    :return: parsed address
    :rtype: list
    """
    parsed = parser.parse(address.upper())
    return parsed


def plotPerformance(countsCorrect, countsAll, outpath='/Users/saminiemi/Projects/ONS/AddressIndex/figs/'):
    """
    Generate a simple bar chart showing the performance of the parser.

    :param countsCorrect:
    :param countsAll:
    :param outpath:

    :return:
    """
    # compute the fractions
    frac = []
    labels = []
    for token in countsCorrect.keys():
        frac.append(float(countsCorrect[token])/countsAll[token]*100.)
        labels.append(token)

    # sort frac and then labels
    frac = np.asarray(frac)
    labels = np.array(labels)
    inds = frac.argsort()
    frac = frac[inds]
    labels = labels[inds]

    # make a simple visualisation
    location = np.arange(len(labels))
    width = 0.5
    fig = plt.figure(figsize=(8, 6))
    plt.title('Parsing Performance: 100k Holdout Sample')
    ax = fig.add_subplot(1, 1, 1)
    plt.barh(location, frac, width, color='g', alpha=0.6)
    for p in ax.patches:
        ax.annotate("%.1f" % p.get_width(), (p.get_x() + p.get_width(), p.get_y()),
                    xytext=(-40, 4), textcoords='offset points', color='white', fontsize=14)

    plt.xlabel('Percent of the Sample Correctly Labelled')
    plt.yticks(location + width/2., labels)
    plt.xlim(0, 100.1)
    plt.tight_layout()
    plt.savefig(outpath + 'tokenParsingPerformance.pdf')
    plt.close()


def sequence_accuracy_score(y_true, y_pred):
    """
    Return sequence accuracy score. Match is counted only when two sequences are equal.

    :param y_true:
    :param y_pred:

    :return:
    """
    total = len(y_true)

    matches = sum(1 for yseq_true, yseq_pred in zip(y_true, y_pred)
                  if list(yseq_true) == list(yseq_pred))

    return matches / total


def checkPerformance(holdoutfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml'):
    """

    :param holdoutfile: location and name of the holdout XML data file
    :type holdoutfile: str

    :return: None
    """
    crf = sklearn_crfsuite.CRF(model_filename=t.MODEL_PATH + t.MODEL_FILE, verbose=True)
    X_test, y_test = t.readData(holdoutfile)

    # store labels
    labels = list(crf.classes_)
    sorted_labels = sorted(labels, key=lambda name: name)

    print('Predicting holdout data...')
    y_pred = crf.predict(X_test)

    print('\nPerformance:')
    # Calculate metrics for each label, and find their average,
    # weighted by support (the number of true instances for each label).
    total = metrics.flat_f1_score(y_test, y_pred, average='weighted', labels=labels)
    # full sequence accuracy
    sequence_accuracy = sequence_accuracy_score(y_test, y_pred)

    print('F1-score:', total)
    print('Sequence accuracy:', sequence_accuracy)

    print("")
    report = metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=3)
    print(report)


def _manual(outputfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/incorrectlyParsed.csv'):
    """
    Predict the tokens for the holdout data and check the performance.

    :param outputfile: name of the output file to store incorrectly parsed addresses
    :type outputfile: str

    :return: None
    """
    correct = 0
    correctItems = 0
    all = 0
    allItems = 0
    countsCorrect = dict()
    countsAll = dict()
    store = []

    print('Predicting holdout data...')
    for raw_string, components in t.readXML('holdout.xml'):
        all += 1

        # get the true labels
        _, true_labels = list(zip(*components))
        true_labels = list(true_labels)

        # parse the raw string
        parsed = predict(raw_string)
        predicted = [x[1] for x in parsed]

        # test whether the full prediction was correct, if not store for inspection
        if true_labels == predicted:
            correct += 1
        else:
            store.append([raw_string, str(true_labels), str(predicted)])

        # loop over the tokens to check which are correct
        for a, b in zip(predicted, true_labels):
            allItems += 1

            if a == b:
                correctItems += 1

            # check for each token separately and store to a dictionary
            for token in t.LABELS:
                if token == b:
                    countsAll[token] = countsAll.get(token, 0) + 1
                    if a == b:
                        countsCorrect[token] = countsCorrect.get(token, 0) + 1

    print('Holdout Addresses:', all)
    print('All Tokens Correct:', correct)
    print('Percent of Correct:', float(correct)/all*100.)
    print('Correct Tokens:', correctItems)
    print('Percent of Tokens Correct:', float(correctItems)/allItems*100.)

    for token in countsCorrect.keys():
        print(float(countsCorrect[token])/countsAll[token]*100.,'percent of', token, 'were correct')

    # # add the all tokens to the dictionaries so that can plot them as well
    # countsAll['All'] = allItems
    # countsCorrect['All'] = correctItems

    print('Generating plots')
    plotPerformance(countsCorrect, countsAll)

    print('Outputting the incorrect ones to a file...')
    fh = open(outputfile, mode='w')
    fh.write('raw, true, predicted\n')
    for line in store:
        fh.write('%s,"%s","%s"\n' % (line[0], line[1], line[2]))
    fh.close()


if __name__ == "__main__":
    # _manual()
    checkPerformance()