from multiprocessing import Process
from queue import Queue
import concurrent.futures
import pandas as pd
import time

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1400)
pd.set_option('display.max_rows', 1000000)

df = pd.read_csv('USvideos.csv')
del df['description'], df['video_error_or_removed'], \
            df['ratings_disabled'], df['comments_disabled'], \
            df['thumbnail_link'], df['category_id'], df['publish_time'], \
            df['tags'], df['channel_title'], df['dislikes']


class Process_one(Process):
    def __init__(self, df, num_of_line):
        super().__init__()
        self.st = time.time()
        self.likes, self.views, self.comment_count = set(), set(), set()
        self.num_of_line = num_of_line
        self.df = df

    def cleaning(self):
        dataset = []
        for aim in ['likes', 'views', 'comment_count']:
            with open('text.txt', 'w') as file:
                file.write(str(self.df.nlargest(self.num_of_line, aim))[119:].replace('MV', ''))
                with open('text.txt', 'r') as f:
                    check = set()
                    for i in f.readlines():
                        check.add(i.replace('      ', ' '))
            if aim == 'likes':
                self.likes = check
            elif aim == 'views':
                self.views = check
            elif aim == 'comment_count':
                self.comment_count = check
        for string in self.likes & self.views & self.comment_count:
            dataset.append(string[string.find(' '):].split())
        return dataset

    def statistic(self):
        ddd = dict()
        data_top = []
        for item in self.cleaning():
            if item[0] not in ddd:
                ddd[item[0]] = item
            else:
                ddd[item[0]][-1] = int(ddd[item[0]][-1]) + int(item[-1])
                ddd[item[0]][-2] = int(ddd[item[0]][-2]) + int(item[-2])
                ddd[item[0]][-3] = int(ddd[item[0]][-3]) + int(item[-3])
        for key in ddd:
            coeff = "%.4f" % (int(ddd[key][-2]) / (int(ddd[key][-3]) + int(ddd[key][-1])))
            data_top.append((ddd[key], coeff))
        return data_top

    def run(self):
        stat = self.statistic()
        stat.sort(key=lambda count: count[-1], reverse=True)
        if len(stat) > 15:
            print('TOP -', 15)
        else:
            print('TOP -', len(stat))

        for item in stat[:15]:
            string = ''

            string += str(stat.index(item) + 1) + ') Relevance coefficient: ' + str(item[1]) + '\n' +\
                      '    Views: ' + str(item[0][-3]) + '\n' + \
                      '    Likes: ' + str(item[0][-2]) + '\n' + \
                      '    Comment_count: ' + str(item[0][-1]) + '\n'
            for n in [-3, -2, -1, 1, 0]:
                item[0].remove(item[0][n])

            print(string + '    Video title: ' + ' '.join(item[0]) + '\n')
        print(time.time() - t, 'время первого')


class Process_two(Process):
    def __init__(self, df, num_of_line):
        super().__init__()
        self.st = time.time()
        self.queue = Queue(10)
        self.num_of_line = num_of_line
        self.likes, self.views, self.comment_count = set(), set(), set()
        self.df = df
        self.ddd = dict()

    def cleaning(self):
        dataset = []
        for aim in ['likes', 'views', 'comment_count']:
            with open('text.txt', 'w') as file:
                file.write(str(self.df.nlargest(self.num_of_line, aim))[119:].replace('MV', ''))
                with open('text.txt', 'r') as f:
                    check = set()
                    for i in f.readlines():
                        check.add(i.replace('      ', ' '))
            if aim == 'likes':
                self.likes = check
            elif aim == 'views':
                self.views = check
            elif aim == 'comment_count':
                self.comment_count = check
        for string in self.likes & self.views & self.comment_count:
            dataset.append(string[string.find(' '):].split())
        return dataset

    def statistic(self, q):
        while True:
            item = q.get()
            if item is None:
                break
            else:
                if item[0] not in self.ddd:
                    self.ddd[item[0]] = item
                else:
                    self.ddd[item[0]][-1] = int(self.ddd[item[0]][-1]) + int(item[-1])
                    self.ddd[item[0]][-2] = int(self.ddd[item[0]][-2]) + int(item[-2])
                    self.ddd[item[0]][-3] = int(self.ddd[item[0]][-3]) + int(item[-3])

    def fin(self, data):
        data_top = []
        for key in data:
            coeff = "%.4f" % (int(data[key][-2]) / (int(data[key][-3]) + int(data[key][-1])))
            data_top.append((data[key], coeff))

        data_top.sort(key=lambda count: count[-1], reverse=True)
        if len(data_top) > 15:
            print('TOP -', 15)
        else:
            print('TOP -', len(data_top))
        for item in data_top[:15]:
            string = ''
            string += str(data_top.index(item) + 1) + ') Relevance coefficient: ' + str(item[1]) + '\n' + \
                      '    Views: ' + str(item[0][-3]) + '\n' + \
                      '    Likes: ' + str(item[0][-2]) + '\n' + \
                      '    Comment_count: ' + str(item[0][-1]) + '\n'
            for n in [-3, -2, -1, 1, 0]:
                item[0].remove(item[0][n])

            print(string + '    Video title: ' + ' '.join(item[0]) + '\n')

    def run(self):
        q = Queue(100000)
        for i in self.cleaning():
            q.put(i)
        q.put(None)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.map(self.statistic(q))
        self.fin(self.ddd)
        print(time.time() - t, 'время второго')


if __name__ == '__main__':
    a = Process_one(df, 11000)
    b = Process_two(df, 11000)

    t = time.time()
    a.start()
    b.start()
    a.join()
    b.join()

