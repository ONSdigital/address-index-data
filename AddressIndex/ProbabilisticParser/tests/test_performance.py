"""
ONS Address Index - Test the Performance of the Probabilistic Parser
====================================================================

A simple script to test the performance of a trained probabilistic parser
using holdout data.


Requirements
------------

:requires: lxml
:requires: addressParser


Author
------

:author: Sami Niemi (sami.niemi@valtech.co.uk)


Version
-------

:version: 0.1
:date: 17-Oct-2016
"""
import addressParser
from lxml import etree
import os
import sys


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
        sequence_components = []
        for component in list(sequence_xml):
            sequence_components.append([component.text, component.tag])

        yield raw_text, sequence_components


def predict(address):
    """

    :param address: raw address string to be parsed
    :type address: str

    :return: parsed address
    :rtype: list
    """
    parsed = addressParser.parse(address.upper())
    return parsed


def runAll(outputfile='/Users/saminiemi/Projects/ONS/AddressIndex/data/incorrectlyParsed.csv'):
    """

    :return: None
    """
    correct = 0
    incorrect = 0
    all = 0
    store = []

    print('Predicting holdout data...')
    for raw_string, components in readHoldoutData('holdout.xml'):
        # get the true labels
        _, true_labels = list(zip(*components))
        true_labels = list(true_labels)

        # parse the raw string
        parsed = predict(raw_string)
        predicted = [x[1] for x in parsed]

        # test whether the prediction was correct, if not store for inspection
        if true_labels == predicted:
            correct += 1
        else:
            incorrect += 1
            store.append([raw_string, str(true_labels), str(predicted)])

        all += 1

    print('All:', all)
    print('Correct:', correct)
    print('Incorrect:', incorrect)
    print('Percent of Correct:', float(correct)/all*100.)

    print('Outputting the incorrect ones to a file...')
    fh = open(outputfile, mode='w')
    fh.write('raw, true, predicted\n')
    for line in store:
        fh.write('%s,"%s","%s"\n' % (line[0], line[1], line[2]))
    fh.close()


if __name__ == "__main__":
    runAll()
