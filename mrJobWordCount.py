from mrjob.job import MRJob

class mrWordCount(MRJob):
     def mapper(self, key,line):
          for word in line.split(' '):
               yield word.lower(), 1
     def reducer(self, word, occurances):
          yield word, sum(occurances)

if __name__ == '__main__':
     mrWordCount.run()
