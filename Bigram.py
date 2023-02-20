import sys
from pyspark import SparkContext, SparkConf
import collections
from collections import deque


# def merge_dict(dict1, dict2):
#     # c = {i: dict1.get(i, 0) + dict2.get(i, 0) for i in set(dict1).union(dict2)}
#     c = dict1 + dict2
#
#     return c


def map_Bigram_Count(line):
    biGrams = []
    words = line.split(" ")
    for i in range(len(words) - 1):
        key = words[i]
        value = (1, [(words[i], words[i + 1])])  # freq of a, (word pair), freq of word pair
        biGrams.append((key, value))
    return biGrams


def reducer(value1, value2):  # reduce
    freq_first_1, list_dict_1 = value1
    freq_first_2, list_dict_2 = value2
    result_freq = freq_first_1 + freq_first_2
    list_dict_1.extend(list_dict_2)
    return result_freq, list_dict_1
    pass


def freq_to_prob(value):  # map
    word, info = value
    freq_of_word, list_probs = info

    counter = collections.Counter(list_probs)

    for key in counter:
        counter[key] = counter[key] / freq_of_word

    return word, (freq_of_word, counter)
    pass


conf = SparkConf()
sc = SparkContext(conf=conf)

words = sc.textFile("wiki.txt").flatMap(map_Bigram_Count)
# words.coalesce(1, shuffle=True).saveAsTextFile("output2")
biGrams_Counts = words.reduceByKey(reducer)

biGrams_Counts = biGrams_Counts.flatMap(freq_to_prob)
# print(biGrams_Counts.collect())
biGrams_Counts.coalesce(1, shuffle=True).saveAsTextFile("output2")

pass
