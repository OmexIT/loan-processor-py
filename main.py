"""
This module processes the loan provided CSV files and aggregates them
Author: Anto
"""
import csv
from datetime import datetime
from collections import defaultdict
from itertools import imap


class Row(object):

    aggr = defaultdict(dict)  # caution should be taken in its size
    inseen = False

    def __init__(self, row=None):
        self.row = row
        self.msisdn, self.network, date, self.product, amount = self._consume_row(
            row)
        self.amount = amount if type(amount) == float else float(amount)
        self.date = datetime.strptime(date, "%d-%b-%Y")
        self.month = self.date.strftime("%m%y")
        self.year = self.date.strftime("%Y")
        # normalization pricipal: only in this tutorial: manage the size of
        # aggr
        self.hash = "{}_{}_{}".format(
            self.network[-1:], self.product[-1:], self.month)

    def _consume_row(self, row):
        """
        Accpets comma separated text or a dictionary as a row from the csv
        """
        if isinstance(row, dict):
            processed_row = row.get("MSISDN"), row.get('Network'), row.get(
                "Date"), row.get("Product"), row.get("Amount")
        else:
            processed_row = tuple(row.split(","))
        return (x.replace("'", "") for x in processed_row)

    def _increment(self):
        rw = self.aggr[self.hash]
        count, total = rw.get("c", 0), rw.get("t", 0.0)
        self.aggr[self.hash]["c"] = count + 1
        self.aggr[self.hash]["t"] = total + self.amount
        self.inseen = True

    def __add__(self, other_row):
        if not self.inseen and self.row:
            self._increment()
        rw = other_row.aggr[other_row.hash]
        count, total = rw.get("c", 0), rw.get("t", 0.0)
        self.aggr[other_row.hash]["c"] = count + 1
        self.aggr[other_row.hash]["t"] = total + other_row.amount
        return self

# data = [Row("27729554427,Network 1,12-Mar-2016,Loan Product 1,1000.00"),
#         Row("27722342551,Network 2,16-Mar-2016,Loan Product 1,1122.00"),
#         Row("27725544272,Network 3,17-Mar-2016,Loan Product 2,2084.00"),
#         Row("27725326345,Network 1,18-Mar-2016,Loan Product 1,3098.00")]


with open("Loans.csv") as csvfile:
    reader = csv.DictReader(csvfile)
    p = reduce(lambda x, y: x + y, imap(lambda x: Row(x), reader))
    results = p.aggr
    with open("Ouput.csv", 'w') as outcsv:
        writer = csv.DictWriter(
            outcsv, fieldnames=["Network", "Product", "Month", "Amount", "Count"])
        writer.writeheader()
        for key in sorted(results.iterkeys(), key = lambda x : int("".join(x.split("_")))):
            row_data = results.get(key)
            [network, product, month] = key.split("_")
            writer.writerow(
                {"Network": "Network {}".format(network),
                 'Product': "Loan Product {}".format(product),
                 'Month': "{}".format(datetime.strptime(month, "%m%y").strftime("%b-%Y")),
                 'Amount': row_data.get("t", 0.0),
                 'Count': row_data.get("c", 0)})