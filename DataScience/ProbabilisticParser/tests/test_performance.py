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

:version: 0.6
:date: 22-Oct-2016
"""
from ProbabilisticParser import parser
import ProbabilisticParser.common.tokens as t
import ProbabilisticParser.common.metrics as metric
import sklearn_crfsuite
from sklearn_crfsuite import metrics
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from collections import Counter


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


def show_values(pc, fmt="%.2f", **kw):
    """

    :param pc:
    :param fmt:
    :param kw:

    :return: None
    """
    pc.update_scalarmappable()
    ax = pc.get_axes()
    for p, color, value in zip(pc.get_paths(), pc.get_facecolors(), pc.get_array()):
        x, y = p.vertices[:-2, :].mean(0)

        if np.all(color[:3] > 0.5):
            color = (0.0, 0.0, 0.0)
        else:
            color = (1.0, 1.0, 1.0)

        ax.text(x, y, fmt % value, ha="center", va="center", color=color, **kw)


def cm2inch(*tupl):
    """
    Specify figure size in centimeter in matplotlib.

    Source: http://stackoverflow.com/a/22787457/395857

    :param tupl:

    :return:
    """
    inch = 2.54
    if type(tupl[0]) == tuple:
        return tuple(i/inch for i in tupl[0])
    else:
        return tuple(i/inch for i in tupl)


def heatmap(AUC, title, xlabel, ylabel, xticklabels, yticklabels, figure_width=40,
            figure_height=20, correct_orientation=False, cmap='RdBu'):
    """
    Generate a heatmap of the classification report information.

    Inspired by:
        - http://stackoverflow.com/a/16124677/395857
        - http://stackoverflow.com/a/25074150/395857

    :param AUC:
    :param title:
    :param xlabel:
    :param ylabel:
    :param xticklabels:
    :param yticklabels:
    :param figure_width:
    :param figure_height:
    :param correct_orientation:
    :param cmap:

    :return: None
    """
    fig, ax = plt.subplots()
    c = ax.pcolor(AUC, edgecolors='k', linestyle='dashed', linewidths=0.2, cmap=cmap)

    # put the major ticks at the middle of each cell
    ax.set_yticks(np.arange(AUC.shape[0]) + 0.5, minor=False)
    ax.set_xticks(np.arange(AUC.shape[1]) + 0.5, minor=False)

    # set tick labels
    ax.set_xticklabels(xticklabels, minor=False)
    ax.set_yticklabels(yticklabels, minor=False)

    # set title and x/y labels
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    # Remove last blank column
    plt.xlim((0, AUC.shape[1]))

    # Turn off all the ticks§
    ax = plt.gca()
    for tick in ax.xaxis.get_major_ticks():
        tick.tick1On = False
        tick.tick2On = False
    for tick in ax.yaxis.get_major_ticks():
        tick.tick1On = False
        tick.tick2On = False

    # Add color bar
    plt.colorbar(c)

    # Add text in each cell
    show_values(c)

    # Proper orientation (origin at the top left instead of bottom left)
    if correct_orientation:
        ax.invert_yaxis()
        ax.xaxis.tick_top()

    # resize figure
    fig = plt.gcf()
    fig.set_size_inches(cm2inch(figure_width, figure_height))


def plotClassificationReport(classification_report, title='Classification report ', cmap='RdBu',
                             outpath='/Users/saminiemi/Projects/ONS/AddressIndex/figs/'):
    """
    Visualise a classification report. Assumes that the report is in the scikit-learn format.

    from http://stackoverflow.com/questions/28200786/how-to-plot-scikit-learn-classification-report

    :param classification_report: a classification report as returned by scikit-learn
    :type classification_report: str
    :param title: title of the figure
    :type title: str
    :param cmap: name of the matplotlib colour map to use
    :type cmap: str
    :param outpath: location to which the output figure is stored
    :type outpath: str

    :return: None
    """
    lines = classification_report.split('\n')

    classes = []
    plotMat = []
    support = []
    class_names = []

    for line in lines[2: (len(lines) - 2)]:
        t = line.strip().split()

        if len(t) < 2:
            continue

        v = [float(x) for x in t[1: len(t) - 1]]

        classes.append(t[0])
        support.append(int(t[-1]))
        class_names.append(t[0])
        plotMat.append(v)

    xlabel = 'Metrics'
    ylabel = 'Address Tokens'
    xticklabels = ['Precision', 'Recall', 'F1-score']
    yticklabels = ['{0} ({1})'.format(class_names[idx], sup) for idx, sup  in enumerate(support)]
    figure_width = 25
    figure_height = len(class_names) + 7
    correct_orientation = False

    heatmap(np.array(plotMat), title, xlabel, ylabel, xticklabels, yticklabels,
            figure_width, figure_height, correct_orientation, cmap=cmap)

    plt.savefig(outpath + 'tokenParsingPerformanceReport.pdf', dpi=200, bbox_inches='tight')
    plt.close()


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



def printTransitions(transFeatures):
    """
    Outputs the token transitions and the associated weight.

    :param transFeatures: counter of model instance transition features

    :return: None
    """
    for (label_from, label_to), weight in transFeatures:
        print("%-6s -> %-7s %0.6f" % (label_from, label_to, weight))


def printStateFeatures(stateFeatures):
    """
    Outputs the features that help to predict a label.

    :param stateSeatures: counter of model instance state features

    :return: None
    """
    for (attr, label), weight in stateFeatures:
        print("%0.6f %-8s %s" % (weight, label, attr))


def checkPerformance(holdoutfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/training/holdout.xml'):
    """
    Checks the performance of the trained model using given holdout data.
    Computes weighted f1-score, sequence accuracy, and a classification report.
    Visualises the classification report.

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
    sequence_accuracy = metric.sequence_accuracy_score(y_test, y_pred)

    print('F1-score:', total)
    print('Sequence accuracy:', sequence_accuracy)

    print("")
    report = metrics.flat_classification_report(y_test, y_pred, labels=sorted_labels, digits=3)
    print(report)

    print('Generating a plot...')
    plotClassificationReport(report)

    print("Likeliest transitions:")
    printTransitions(Counter(crf.transition_features_).most_common(15))

    print("Least likely transitions:")
    printTransitions(Counter(crf.transition_features_).most_common()[-15:])

    print("Top 30 positive features:")
    printStateFeatures(Counter(crf.state_features_).most_common(30))

    print("Top 30 negative features:")
    printStateFeatures(Counter(crf.state_features_).most_common()[-30:])


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
    checkPerformance()
