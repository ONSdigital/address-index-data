"""
ONS Address Index - Test the Performance of the Probabilistic Parser
====================================================================

A simple script to test the performance of a trained probabilistic parser
using holdout data. Computes the number of tokens that were correctly identified.
In addition, computes the fraction of complete addresses correctly parsed and
the performance metric per token type.


Requirements
------------

:requires: lxml
:requires: addressParser
:requires: seaborn
:requires: matplotlib
:requires: numpy


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.2
:date: 19-Oct-2016
"""
import addressParser
from lxml import etree
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os
import sys
import operator


# set seaborn style
sns.set_style("whitegrid")
sns.set_context("poster")
sns.set(rc={"figure.figsize": (12, 12)})
sns.set(font_scale=1.5)


def _stripFormatting(collection):
    """
    Clears formatting for an xml collection.

    :param collection:

    :return:
    """
    collection.text = None
    for element in collection:
        element.text = None
        element.tail = None

    return collection


def readHoldoutData(xmlFile):
    """

    :param xmlFile:
    :return:
    """
    component_string_list = []

    # loop through xml file
    if os.path.isfile(xmlFile):
        with open(xmlFile, 'r+') as f:
            tree = etree.parse(f)
            file_xml = tree.getroot()
            file_xml = _stripFormatting(file_xml)
            for component_etree in file_xml:
                # etree components to string representations
                component_string_list.append(etree.tostring(component_etree))
    else:
        print('WARNING: %s does not exist' % xmlFile)
        sys.exit(-9)

    # loop through unique string representations
    for component_string in component_string_list:
        # convert string representation back to xml
        sequence_xml = etree.fromstring(component_string)
        raw_text = etree.tostring(sequence_xml, method='text', encoding='utf-8')
        raw_text = str(raw_text, encoding='utf-8')
        sequence_components = []

        for component in list(sequence_xml):
            sequence_components.append([component.text, component.tag])

        yield raw_text, sequence_components


def predict(address):
    """
    Predict the tokens using a trained probabilistic parser model.
    The address parser must have been trained before parse can be called.

    :param address: raw address string to be parsed
    :type address: str

    :return: parsed address
    :rtype: list
    """
    parsed = addressParser.parse(address.upper())
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
    for token in countsAll:
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


def runAll(outputfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/incorrectlyParsed.csv'):
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
    for raw_string, components in readHoldoutData('holdout.xml'):
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
            for token in addressParser.LABELS:
                if token == b:
                    countsAll[token] = countsAll.get(token, 0) + 1
                    if a == b:
                        countsCorrect[token] = countsCorrect.get(token, 0) + 1

    print('Holdout Addresses:', all)
    print('All Tokens Correct:', correct)
    print('Percent of Correct:', float(correct)/all*100.)
    print('Correct Tokens:', correctItems)
    print('Percent of Tokens Correct:', float(correctItems)/allItems*100.)

    for token in addressParser.LABELS:
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
    runAll()
