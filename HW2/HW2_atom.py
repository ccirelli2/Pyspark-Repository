import os
os.chdir('/home/ccirelli2/Desktop/Scalable_Analytics/HW2/')
#print(os.listdir())

File = open('Amazon_Comments.csv')
Text = File.read()

for line in Text:
    Split = line.split('\n')
    print(Split)
