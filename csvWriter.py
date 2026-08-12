import csv
import json
import os
import shutil
import sqlite3
from time import time

import timetable


def execute(filePath):

    with open(filePath + ".json", "r") as file:

        timetable = json.load(file)

    longest = {}

    for run in timetable:
        total = -1
        direction = timetable[run]["Direction"]
        for stop in timetable[run]:
            total += 1

        if direction not in longest:
            longest[direction] = (run, total)
        elif total > longest[direction][1]:
            longest[direction] = (run, total)


    for direction in longest:
        fileOut = [['']]
        if timetable[longest[direction][0]]["Direction"] != direction:
            continue
        stops = [s for s in timetable[longest[direction][0]] if s != "Direction"]
        print(timetable[longest[direction][0]]["Direction"])
        while len(fileOut) <= len(stops):
            fileOut.append([])

        for index, stop in enumerate(stops, start=1):
            fileOut[index].append(stop)

        for run in timetable:

            if timetable[run]["Direction"] != direction:
                continue
            fileOut[0].append(run)
            rn = timetable[run]
            for stop in rn:
                stp = rn[stop]

                for row in fileOut:

                    if row[0] == stop:
                        row.append(stp["Departure"])

            for row in fileOut:

                if len(row) != len(fileOut[0]):
                    row.append("N/A")

        print(fileOut)
        base_dir = os.path.dirname(filePath)
        base_name = os.path.basename(filePath)
        out_name = f"{base_name}_{direction}.csv"
        out_path = os.path.join(base_dir, out_name)

        with open(out_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(fileOut)   

        move_csv(filePath, direction)

def move_csv(filePath, direction):
    base_dir = os.path.dirname(filePath)
    base_name = os.path.basename(filePath)

    csv_dir = os.path.join(base_dir, "csv")
    os.makedirs(csv_dir, exist_ok=True)

    src = os.path.join(base_dir, f"{base_name}_{direction}.csv")
    dst = os.path.join(csv_dir, f"{base_name}_{direction}.csv")

    if os.path.exists(src):
        shutil.move(src, dst)


if __name__ == "__main__":
    for file in os.listdir(r"D:\Programming\python\TransXchangeParser\.out\ECBU"):
        if file.endswith(".json"):
            execute(r"D:\Programming\python\TransXchangeParser\.out\ECBU\\"+file[:-5])