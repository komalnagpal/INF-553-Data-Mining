import sys,csv
from pyspark import SparkContext
import itertools
import collections
import math


class Apriori:
    def __init__(self, input_file, support_threshold, output_file,
                 num_of_partitions=1):
        self.transaction_file = input_file
        self.support_threshold = float(support_threshold)
        self.output_file = output_file
        self.num_of_partitions = num_of_partitions
        self.candidate_set = frozenset()
        self.frequent_item_sets = set()

    def apriori_process(self):
        global_frequent_set = []
        sc = SparkContext(appName="hw2")
        rdd = sc.textFile(self.transaction_file)
        self.candidate_set = frozenset(rdd.mapPartitions(self.calculate_frequent_item_sets).collect())
        c = sc.parallelize(rdd.mapPartitions(self.find_global_frequent_sets).collect()).flatMap(lambda x: x.items()).reduceByKey(lambda x,y: x+y).collect()
        s_T = self.support_threshold * rdd.count()
        for item,v in c:
            if v >= s_T:
                global_frequent_set.append(sorted(list(item)))
        output_file = open(self.output_file,"w")
        file_writer = csv.writer(output_file)
        for frequent_items in sorted(global_frequent_set):
            file_writer.writerow(frequent_items)

    def find_global_frequent_sets(self,transaction_list):
        candidate_set_count = collections.defaultdict(int)
        for transaction in transaction_list:
            basket = frozenset(transaction.split(','))
            for candidate in self.candidate_set:
                if candidate.issubset(basket):
                    candidate_set_count[candidate] += 1
        yield candidate_set_count


    def calculate_frequent_item_sets(self, baskets_list):
        item_set = set()
        baskets = []
        for transaction in baskets_list:
            items = transaction.split(',')
            baskets.append(frozenset(items))
            for item in items:
                item_set.add(frozenset([item]))
        one_frequent_item_set = self.filter_items_with_minimum_threshold(
            item_set, baskets)
        self.frequent_item_sets.update(one_frequent_item_set)
        current_frequent_set = one_frequent_item_set
        ith_item_set = 2
        while len(current_frequent_set) > 0:
            candidate_set = self.construct_candidate_sets(
                current_frequent_set, ith_item_set)
            current_frequent_set = self.filter_items_with_minimum_threshold(
                candidate_set, baskets)
            self.frequent_item_sets.update(current_frequent_set)
            ith_item_set += 1
        return self.frequent_item_sets

    def construct_candidate_sets(self, frequent_set, kth):
        new_candidate_set = set()
        for item_k in frequent_set:
            for item_j in frequent_set:
                new_candidate = item_k.union(item_j)
                if len(new_candidate) == kth:
                    new_candidate_set.add(new_candidate)
        return new_candidate_set

    def filter_items_with_minimum_threshold(self, item_set, baskets):
        support_threshold = math.ceil(
            self.support_threshold * len(baskets))
        item_counts = collections.defaultdict(int)
        frequent_item_set = set()
        for basket in baskets:
            for item in item_set:
                if item.issubset(basket):
                    item_counts[item] += 1
        for item, count in item_counts.items():
            if count >= support_threshold:
                frequent_item_set.add(item)
        return frequent_item_set

def main():
    input_file, support_threshold, output_file = sys.argv[1:]
    apriori_executor = Apriori(input_file, support_threshold, output_file)
    apriori_executor.apriori_process()


if __name__ == "__main__":
    main()

