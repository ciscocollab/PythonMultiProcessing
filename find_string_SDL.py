import os
import re
import time
import gzip
import io
import gc
import datetime
from multiprocessing import Process, Manager


class SdlFiles:  # all files will be instantiated as a file object
    def __init__(self, path="", file_name="default_name.txt", unix_creation_time="490104000"):
        self.path = path  # store the file's path
        self.name = file_name  # store the file's name
        self.created = unix_creation_time  # store the file's creation time


def get_sdl_files(passed_dir):
    """
    This function finds all the CCM SDL files
    :param passed_dir: This is the CWD of where the python script is ran from
    """
    for rootdir, subdirs, files in os.walk(passed_dir):  # Checking the dir/subdirs for SDL files
        for file in files:
            file_path = os.path.join(rootdir, file)  # full file path stored as 'file_path'
            match = re.search(regex_file, file_path)  # checking if the file is CCM SDL
            if match:  # if the file is CCM SDL, then execute the code below
                create_time = str(os.path.getmtime(file_path))  # find out when the file was created, this way we read the files in the correct order
                real_name = match.group(1)  # the name of the file without the path (i.e. 'SDL003_100_000491.txt')
                new_file = SdlFiles(file_path, real_name, create_time)  # instantiate the file object for the current file being handled
                print("Number of initialized files: {}".format(len(file_objects) + 1))  # notify the consumer how many files have been initialized.
                file_objects.append(new_file)  # append the new object to the list 'file_objects'


def find_lines_func(quarter_to_check_passed, list_to_update_passed, found_lines_passed, provided_string_passed):
    """
    This function opens the compressed gz files, reads them with io.BufferedReader to increase speed, then checks
    for lines which contain our provided substring.

    When a match is found, the

    :param quarter_to_check_passed: This defines the 25% of logs to review (all logs from start to stop for 25% segmented work)
    :param list_to_update_passed: Will document how many files the subprocess reviewed. Stops parent process from moving forward until all children are done.
    :param found_lines_passed: global dictionary housing matched lines and their file
    :param provided_string_passed: This is the search string provided by the user.
    """

    sub_pid = os.getpid()  # running this within the function that is called by multiprocessing.Process so that
    # I can get the pid of the subprocess

    lines_found_local = dict()  # this will be updated locally then passed back to the parent later to update 'found_lines'
    files_checked_local = []  # this will be updated locally then passed back to the parent later to update whatever list was passed for updating (files_checked_1, files_checked_2, etc.)
    how_many_checked = 0  # each sub process will update this value. Later this value will be used to determine the progress of each sub process.

    try:

        for file_obj in quarter_to_check_passed:  # iterate through the objects passed to the function find_lines_func()

            logs = gzip.open(file_obj.path, 'rb')  # open the gz file
            fil = io.BufferedReader(logs)  # read the file using buffer

            try:

                for line in fil:  # iterate through each line of the file
                    line_str = str(line)  # convert the line to a string rather than bytes or whatever --- This didn't work: line_str = line.decode('utf-8')

                    if (len(line_str) > 170) and (len(line_str) < 190):  # "alerting_call_collection_PickupCallLocateRes - alertingTime" lines fall between this range from what I can tell - room for variation included
                        if provided_string_passed.lower() in line_str.lower():  # check if the desired string is found in the line
                            print("FOUND LINE FOUND LINE FOUND LINE FOUND LINE FOUND LINE FOUND LINE FOUND LINE ")
                            lines_found_local.update({line_str: file_obj.path})  # add entry to dict 'lines_found_local' that includes the line and the path to the file where the string was found.

                how_many_checked += 1  # after each file is done, increase the counter on this variable
                percent_complete_for_sub_proc = how_many_checked / len(quarter_to_check_passed) * 100  # do some math to get the percentage of 'how_many_checked' out of 'len(quarter_to_check_passed)'
                print(f"\n\nprocess {sub_pid} is {round(percent_complete_for_sub_proc)} % complete")  # print the percentage to the terminal as a rounded value

            except OSError as e:

                files_checked_local.append("1")  # since we checked a file we are updating the number of files we checked
                continue

            logs.close()  # close the file before moving forward
            files_checked_local.append("1")  # since we checked a file we are updating the number of files we checked

            print(f"Still working on it, process {sub_pid} just completed file \n\t{file_obj.path}.")  # using this for now instead of the one that shows % complete. going to be hard to get % with multiprocessing
            gc.collect()  # release unreferenced memory, huge help with managing the program's memory consumption

    except:

        files_checked_local.append("1")  # since we checked a file we are updating the number of files we checked

    found_lines_passed.update(lines_found_local)  # update the global dictionary to include the local dictionary of matched line and the file
    list_to_update_passed.extend(files_checked_local)  # update the global list to include the local list - used to track if all files were checked


if __name__ == '__main__':  # execute the code below only if it is the main process
    # (prevents subprocess from executing code when using multiprocessing)

    provided_string = input("\n\nWhat is the string you are searching for? ")

    start_time = time.time()  # will be used to determine the full run time

    pwd = os.getcwd()  # used later to search the current directory, and it's subdirectories, for CCM SDL files.

    file_objects = []  # when the files are instantiated, their place in memory is added to this list.

    regex_file = re.compile(r".*cm\\trace\\ccm\\sdl\\(SDL.*).gz")  # This regex string helps find CCM SDL files only
    get_sdl_files(pwd)  # executing the function to actually find all the .gz SDL files

    manager = Manager()  # initialize the multiprocessing.Manager - Manager allows us to get information from the subprocesses back to the parent.

    files_checked_1 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_2 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_3 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    files_checked_4 = manager.list()  # this will be used later to stop the __main__ from executing until files_checked = len(file_objects)
    total_files_checked = len(files_checked_1) + len(files_checked_2) + len(files_checked_3) + len(files_checked_4)  # this will later be used to see if all files were reviewed

    found_lines = manager.dict()  # this dictionary will house all the matched lines and the file where they were found

    """
    I want to be able to divide the work into quarters and allow sub-processes to handle the segmented work.

    So I will take the total number of files and divide it by 4 and store the value as 'first_quarter', then I
    will create the second_quarter and third_quarter based off first_quarter.

    Using the variables first_quarter/second_quarter/third_quarter I will use list slicing to tell the function
    'find_lines_func' which parts of the list it should be concerned with.

    Fourth quarter isn't needed as I can just say go from the end of the 3rd quarter to the end of the list.
    """
    first_quarter = len(file_objects) / 4
    second_quarter = first_quarter * 2
    third_quarter = first_quarter * 3

    """
    This is where the magic really happens. The 4 processes are initialized and stored as p1/p2/p3/p4.
    The function called is find_lines_func() and the sliced quarters of 'file_objects' are sent with other parameters as well.
    """
    p1 = Process(target=find_lines_func, args=(file_objects[0:round(first_quarter)], files_checked_1, found_lines, provided_string))

    p2 = Process(target=find_lines_func, args=(file_objects[round(first_quarter):round(second_quarter)], files_checked_2, found_lines, provided_string))

    p3 = Process(target=find_lines_func, args=(file_objects[round(second_quarter):round(third_quarter)], files_checked_3, found_lines, provided_string))

    p4 = Process(target=find_lines_func, args=(file_objects[round(third_quarter)::], files_checked_4, found_lines, provided_string))

    # We already instantiated the processes; however, we now need to start them
    p1.start()

    p2.start()

    p3.start()

    p4.start()

    while total_files_checked != len(file_objects):  # don't execute more code until this criteria is met
        total_files_checked = len(files_checked_1) + len(files_checked_2) + len(files_checked_3) + len(files_checked_4)  # Check the math each loop as 'total_files_checked' will be updated by subprocesses
        gc.collect()  # release unreferenced memory, huge help with managing memory consumption

        # print(f"\n\n{total_files_checked} does not equal {len(file_objects)}... keep waiting.")  # This looks weird when printed, it is important though. When the script is finishing large amounts of logs, this line lets it be known that the script is still working.
        continue  # keep running through the loop until the criteria is met

    print("\nReading the files is complete. Please wait while we write the results to a text file.\n")

    date_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    name_of_file = date_time + "__SDL_search_results.txt"
    f = open(name_of_file, "w+")  # this is where the results of the script will be documented. All processes that were started, without stopping, will be written to this file.

    if found_lines:
        to_write = [f"{l} was started in the file below:\n   {f} \n\n" for l, f in found_lines.items()]  # So that I only have to perform 1 write operation I will store everything in a variable named 'to_write'
    else:
        to_write = "No match found.\n\n"

    f.writelines(to_write)  # this is where we actually write to the file

    def how_long():
        """
        This function identifies the current time,
        then subtracts the start time,
        then returns the diff so we can document the program's total runtime.
        """
        duration = time.time() - start_time
        duration = duration / 60
        return round(duration, 1)

    runtime = how_long()

    f.write("\n----------\n----------\n")
    f.write(f"It took {runtime} minutes for the script to complete.")  # Document the runtime in the file.
    f.close()  # close the file

    # The rest tells the consumer about the runtime in the terminal and the location of the file where the script output is stored

    print(f"\nIt took {runtime} minutes for the script to complete.\n")
    print(f"The output from the script is stored in a file named '{name_of_file}' in the directory below:\n")
    print("    ", os.getcwd())
