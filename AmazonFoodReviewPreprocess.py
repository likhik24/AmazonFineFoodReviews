from itertools import islice
import re
import sys
import pandas as pd
import numpy as np

#Function to read the food.txt(Amazon food review) file
#Convert each reivew based on counterToDelimit to rows in csv file
def readTextEachReview(textReview, counterToDelimit):
    with open(textReview, encoding ='ISO-8859-1') as reader:
        while True:
            rows = list(islice(reader, counterToDelimit))
            if rows:
                yield rows
            else:
                break

#Function to remove null values in Summary column
#Dropping the not helpful columns
def removeNull(file):
    data_values = pd.read_csv(file,index_col=False)
    # dropping few columns which are not helpful in prediction of functionality of pumps based on co-relation plot
    data_values = data_values.dropna(subset=["Summary"])
    data_val = data_values.drop(["Helpfulness", "Text", "ProfileName"], axis=1)
    data_val.to_csv(sys.argv[3], index = False)

#Function to get each value of a single review
def getLineOfEachReview(line):
    if line == "\n":
        return line
    return line.split(":")[1].strip()

# Main function for pre-processing the Amazon food review text file
if __name__ == "__main__":

    with open(sys.argv[2], "w", encoding ='ISO-8859-1') as csvWrite:
        headerRow = "ProductId" + "," + "UserId" + "," + "ProfileName" + "," + "Helpfulness" + "," + "Score" + "," + "Time" + "," + "Summary" + "," + "Text" + "\n"
        csvWrite.write(headerRow)
        for review in readTextEachReview(sys.argv[1],9):
            csvFill = ""
            for value in review:
                if "review/summary:" in value or "review/text:" in value or "review/profileName:" in value:
                    data = getLineOfEachReview(value)
                    addToCsv = re.sub("[^a-zA-Z]", " ", data)
                elif "product/productId:" in value or "review/userId:" in value or "review/score:" in value or "review/time:" in value or (value == "\n"):
                    addToCsv = getLineOfEachReview(value)
                #Helpfulness not helpful
                elif "review/helpfulness:" in value:
                    addToCsv = " "
                else:
                    continue
                #if endOfLine add it as a row of new CSV file
                if addToCsv == "\n" :
                     csvFill += addToCsv
                else:
                    csvFill += addToCsv + ","
            #Write to CSV file
            csvWrite.write(csvFill)
        #Finally remove null and drop not helpful columns
        removeNull(sys.argv[2])
