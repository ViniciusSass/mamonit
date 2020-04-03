#!/usr/bin/env python3

from datetime import datetime
import re
import os
import sys
import argparse
import operator

# TODO: support user names with spaces
# TODO: support log dirs in show-running-campaigns


class CampaignExecution:
    def __init__(self, camp_name=None, datetime_begin=None, thread=None, user=None, datetime_end=None,
                 status=None, sasserver6=None):
        self.camp_name = camp_name
        self.datetime_begin = datetime_begin
        self.thread = thread
        self.user = user
        self.datetime_end = datetime_end
        self.status = status
        self.sasserver6 = sasserver6

    def __repr__(self):
        return self.camp_name + " " + str(self.datetime_begin) + " " + str(self.datetime_end) + " " + self.thread \
               + " " + self.user + " " + str(self.status) + " " + str(self.sasserver6)


class FinishingEvents:
    def __init__(self, datetime_event=None, thread=None, status=None):
        self.datetime_event = datetime_event
        self.thread = thread
        self.status = status

    def __repr__(self):
        return str(self.datetime_event) + " " + self.thread + " " + self.status


class MamonitError(Exception):
    pass


def read_campaign_executions_from_logs(log_file_names, sasserver6_instance):
    campaign_executions = []
    finishing_events = []
    restart_datetimes = []
    for log_file_name in log_file_names:
        new_campaign_executions, new_finishing_events, new_restart_datetimes = \
            parse_events_from_log(log_file_name, sasserver6_instance)
        campaign_executions += new_campaign_executions
        finishing_events += new_finishing_events
        restart_datetimes += new_restart_datetimes
    set_campaigns_finishing_time_from_finishing_events(campaign_executions, finishing_events, restart_datetimes)
    check_campaigns_killed_by_restart(campaign_executions, restart_datetimes)
    return campaign_executions


def create_concurrency_data_structure(campaign_executions):
    concurrency_data_structure = []
    for c in campaign_executions:
        new_entry = [c.datetime_begin, "start"]
        concurrency_data_structure.append(new_entry)
        if c.datetime_end is not None:
            new_entry = [c.datetime_end, "finish"]
            concurrency_data_structure.append(new_entry)
    concurrency_data_structure.sort(key=operator.itemgetter(0))
    return concurrency_data_structure


def concurrency_analysis(log_dir_or_file, sasserver6_instance, output_file):
    # Test if it's a file or dir
    if args.log_dir:
        log_file_names = get_log_file_names(log_dir_or_file)
    else:
        log_file_names = [log_dir_or_file]
    campaign_executions = read_campaign_executions_from_logs(log_file_names, sasserver6_instance)
    concurrency_data_structure = create_concurrency_data_structure(campaign_executions)
    campaigns_running_count = 0
    concurrency_analysis_list = []
    for c in concurrency_data_structure:
        if c[1] == "start":
            campaigns_running_count += 1
        else:
            campaigns_running_count -= 1
        concurrency_analysis_list.append([c[0], campaigns_running_count])
    if output_file is None:
        print("Datetime\tConcurrent campaigns\tSASServer6 instance")
        for c in concurrency_analysis_list:
            print(str(c[0]) + "\t" + str(c[1]) + "\t" + sasserver6_instance)
    else:
        with open(output_file, "w") as of:
            for c in concurrency_analysis_list:
                of.write(str(c[0]) + "," + str(c[1]) + "," + sasserver6_instance + "\n")


def get_log_file_names(log_dir):
    log_file_names = []
    for f in os.listdir(log_dir):
        if re.findall("SASCustIntelCore.*log", f):
            log_file_names.append(log_dir + "/" + f)
    return log_file_names


def parse_events_from_log(log_file_name, sasserver6_instance):
    with open(log_file_name, "r", encoding="utf-8") as log_file:
        campaign_executions = []
        finishing_events = []
        sasserver6_restart_datetimes = []
        for line in log_file:
            if re.findall("has capability MAExecuteCampaign", line):
                aux1 = re.split("etc. ", line)
                camp_name = aux1[1][:len(aux1[1]) - 1]
                aux2 = re.split("\s", aux1[0])
                datetime_begin = datetime.strptime(aux2[0] + " " + aux2[1], "%Y-%m-%d %H:%M:%S,%f")
                thread = aux2[3][1:len(aux2[3]) - 1]
                user = aux2[5][1:len(aux2[5]) - 1]
                campaign_execution = CampaignExecution(camp_name=camp_name, datetime_begin=datetime_begin,
                                                       thread=thread, user=user, sasserver6=sasserver6_instance)
                campaign_executions.append(campaign_execution)
            elif re.findall(r"Executed list of communications with error level:|Exception executing campaign|"
                            r"Encountered client exception", line):
                if re.search("exception", line, re.IGNORECASE):
                    status = "FAIL"
                else:
                    status = line.split(" ")[-3:][0]
                aux = line.split()
                datetime_event = datetime.strptime(aux[0] + " " + aux[1], "%Y-%m-%d %H:%M:%S,%f")
                thread = aux[3][1:len(aux[3]) - 1]
                finishing_events.append(FinishingEvents(datetime_event, thread, status))
            elif re.findall("ServiceURL DAO not set. Not running in mid-tier.", line):
                aux = line.split(" ")
                dttm = datetime.strptime(aux[0] + " " + aux[1], "%Y-%m-%d %H:%M:%S,%f")
                sasserver6_restart_datetimes.append(dttm)
        return campaign_executions, finishing_events, sasserver6_restart_datetimes


def set_campaigns_finishing_time_from_finishing_events(campaign_executions, finishing_events, restart_datetimes):
    campaign_executions.sort(key=operator.attrgetter("datetime_begin"))
    finishing_events.sort(key=operator.attrgetter("datetime_event"))
    for c in campaign_executions:
        i = 0
        flag_found = False
        nearest_restart = get_nearest_restart(c, restart_datetimes)
        while i < len(finishing_events) and not flag_found:
            if c.thread == finishing_events[i].thread and c.datetime_begin < finishing_events[i].datetime_event:
                # Prevent campaign that was interrupted by restart to rob other campaign's finishing event
                if nearest_restart is not None and nearest_restart < finishing_events[i].datetime_event:
                    c.datetime_end = nearest_restart
                    c.status = "FAIL"
                    flag_found = True
                else:
                    c.datetime_end = finishing_events[i].datetime_event
                    c.status = finishing_events[i].status
                    finishing_events.remove(finishing_events[i])
                    flag_found = True
            i += 1


def check_campaigns_killed_by_restart(campaign_executions, restart_datetimes):
    restart_datetimes.sort()
    for c in campaign_executions:
        if c.datetime_end is None:
            flag_found = False
            i = 0
            while i < len(restart_datetimes) and not flag_found:
                if c.datetime_begin < restart_datetimes[i]:
                    c.datetime_end = restart_datetimes[i]
                    c.status = "FAIL"
                    flag_found = True
                i += 1


def get_nearest_restart(campaign_execution, restart_datetimes):
    for r in restart_datetimes:
        if r > campaign_execution.datetime_begin:
            return r


def show_running_campaigns(log_file, sasserver6_instance):
    campaign_executions = read_campaign_executions_from_logs([log_file], sasserver6_instance)
    print("Campaign name\tStart datetime\tUser\tSASServer6 instance")
    for c in campaign_executions:
        if c.datetime_end is None:
            print("%s\t%s\t%s\t%s" % (c.camp_name, c.datetime_begin, c.user, sasserver6_instance))


def extract_campaign_executions(log_dir_or_file, sasserver6_instance, output_file):
    # Test if it's a file or dir
    if args.log_dir:
        log_file_names = get_log_file_names(log_dir_or_file)
    else:
        log_file_names = [log_dir_or_file]
    campaign_executions = read_campaign_executions_from_logs(log_file_names, sasserver6_instance)
    if output_file:
        with open(output_file, "w") as of:
            for c in campaign_executions:
                of.write("%s,%s,%s,%s,%s\n" % (c.camp_name, c.datetime_begin, c.user, c.status, sasserver6_instance))
    else:
        print("Campaign name\tStart datetime\tUser\tStatus\tSASServer6 instance")
        for c in campaign_executions:
            print("%s\t%s\t%s\t%s\t%s" % (c.camp_name, c.datetime_begin, c.user, c.status, sasserver6_instance))


def merge_concurrency_analysis(analysis_files, output_file):
    concatenated_analaysis = merge_analysis_files(analysis_files)
    concatenated_analaysis.sort(key=operator.itemgetter(0))
    instances = get_instances_from_analysis_data_structure(concatenated_analaysis)
    instances_with_count = {}
    for i in instances:
        instances_with_count[i] = 0
    merged_analysis = []
    for i in concatenated_analaysis:
        dttm = i[0]
        count = i[1]
        instance = i[2]
        instances_with_count[instance] = int(count)
        count_sum = 0
        for j in instances_with_count:
            count_sum += instances_with_count[j]
        new_line = [dttm, count_sum]
        merged_analysis.append(new_line)
    if output_file:
        with open(output_file, "w", encoding="utf-8") as of:
            for i in merged_analysis:
                of.write("%s,%s\n" % (i[0], i[1]))
    else:
        print("Datetime\tCampaign count")
        for i in merged_analysis:
            print("%s\t%s" % (i[0], i[1]))


def merge_analysis_files(analysis_files):
    merged_list = []
    for i in analysis_files:
        with open(i, "r", encoding="utf") as file:
            current_file = []
            for line in file:
                dttm = line.split(",")[0]
                campaign_count = line.split(",")[1]
                instance_name = line.split(",")[2][:-1]  # remove new-line character
                new_line = [dttm, campaign_count, instance_name]
                current_file.append(new_line)
            merged_list += current_file
    return merged_list


def get_instances_from_analysis_data_structure(analysis_data_structure):
    instances = set()
    for line in analysis_data_structure:
        if line[2] not in instances:
            instances.add(line[2])
    return instances


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["show-running-campaigns", "concurrency-analysis",
                                           "extract-campaign-executions", "merge-concurrency-analysis"],
                        help="action to be run")
    parser.add_argument("--log-dir", help="log directory")
    parser.add_argument("--log-file", help="log file")
    parser.add_argument("--instance-name", help="SASServer6 instance name")
    parser.add_argument("--output-file", help="output results to file in CSV format")
    parser.add_argument("--analysis-files", nargs="+", help="concurrency analysis files to be merged")
    args = parser.parse_args()
    try:
        if args.action == "concurrency-analysis":
            if args.instance_name is None:
                raise MamonitError("error: specify the SASServer6 instance name.")
            if args.log_dir is None and args.log_file is None:
                raise MamonitError("error: specify either a log file or a log directory.")
            elif args.log_file:
                concurrency_analysis(args.log_file, args.instance_name, args.output_file)
            else:
                concurrency_analysis(args.log_dir, args.instance_name, args.output_file)
        elif args.action == "show-running-campaigns":
            if args.instance_name is None:
                raise MamonitError("error: specify the SASServer6 instance name.")
            if args.log_file is None:
                raise MamonitError("error: show-running-camps only supports log files.")
            show_running_campaigns(args.log_file, args.instance_name)
        elif args.action == "extract-campaign-executions":
            if args.instance_name is None:
                raise MamonitError("error: specify the SASServer6 instance name.")
            if args.log_dir is None and args.log_file is None:
                raise MamonitError("error: specify either a log file or a log directory.")
            elif args.log_file:
                extract_campaign_executions(args.log_file, args.instance_name, args.output_file)
            else:
                extract_campaign_executions(args.log_dir, args.instance_name, args.output_file)
        elif args.action == "merge-concurrency-analysis":
            if args.analysis_files is None:
                raise MamonitError("error: specify the concurrency analysis files.")
            merge_concurrency_analysis(args.analysis_files, args.output_file)
    except MamonitError as m:
        parser.print_usage()
        print(m)
        sys.exit(1)
