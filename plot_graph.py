import sys
import matplotlib.pyplot as plt
import numpy as np


def list_elmts(list):
    start = 0
    new_lst = []
    for item_cat in list:
        start = start + 1
        new_lst.append(start)
    return new_lst


# Dividing Line Sequence
count = 0
repeat = 5


# Identifying Sequence
sequence= ['Climate','Cannabis','Cryptocurrency','Exams','Marketing']

with open('result.txt') as f:
    lines = f.readlines()
    x = [line.split()[0] for line in lines]
    y = [float(line.split()[1]) for line in lines]

# Identify when all categories have been found
# Specifically iterate through list to identify the first instance of sublist by index
print(x)
repeating_seq = min([i for i in range(len(x)) if x[i:i+len(sequence)] == sequence])

# Recreate a list to locate the first sequence of all categories
cat = x[repeating_seq:]
sent_avg = y[repeating_seq:]

# Remove last element in list
sent_avg.pop()

# Creating Plot for X & Y axis of plot
Climate = sent_avg[0:len(sent_avg):5]
Climate_lst = list_elmts(Climate)

Crypto = sent_avg[1:len(sent_avg):5]
Crypto_lst = list_elmts(Crypto)

Cannabis = sent_avg[2:len(sent_avg):5]
Cannabis_lst = list_elmts(Cannabis)

Exams = sent_avg[3:len(sent_avg):5]
Exams_lst = list_elmts(Exams)

Marketing = sent_avg[4:len(sent_avg):5]
Marketing_lst = list_elmts(Marketing)


plt.plot(Climate_lst, Climate, color='green',linewidth=1.0,label='Climate')
plt.plot(Crypto_lst, Crypto, color='orange',linewidth=1.0, label='CryptoCurrency')
plt.plot(Cannabis_lst, Cannabis, color='red',linewidth=1.0,label='Cannabis')
plt.plot(Exams_lst, Exams, color='blue',linewidth=1.0,label='Exams')
plt.plot(Marketing_lst,Marketing, color='black',linewidth=1.0, label='Marketing')


plt.xlabel('Frequency')
plt.ylabel('Average Polarity')
plt.xticks(rotation=90)



plt.legend()
plt.title('Twitter Stream Categories by Frequency in time interval')
plt.show()
