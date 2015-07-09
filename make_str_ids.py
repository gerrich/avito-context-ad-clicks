#!/usr/bin/env python
# -*- coding:utf8 -*-

import csv
import sys
import re
if __name__=="__main__":
  reader = csv.reader(sys.stdin, delimiter='\t', quotechar='"')
  writer = csv.writer(sys.stdout, delimiter='\t', quotechar='"')
  header = reader.next()
  print '\t'.join(header)
  id_mask = [1 if re.search(r'ID$', f) else 0 for f in header]
  for row in reader:
    row = ["%09u"%int(row[i]) if id_mask[i] else row[i] for i in range(len(row)) ]
    #print '\t'.join(row)
    writer.writerow(row)
