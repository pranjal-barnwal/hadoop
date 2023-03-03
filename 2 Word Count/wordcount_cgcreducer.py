#!/usr/bin/env python
import sys
words = {}
# input comes from STDIN
for word in sys.stdin:
	word = word.split(":")[0].strip()
	if word in words:
		words[word] += 1
	else:
		words[word] = 1
for word in words:
	print"%s:%s" % (word, words[word])

