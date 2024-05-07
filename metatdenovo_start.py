#!/usr/bin/env python3

import argparse
import json
import logging
import os
import time
from metatdenovo_report import make_json_report
from metatdenovo_step import monitor_execution

def make_csv_file(csv_file:str, sample_list:list) -> None:
    csv_header = 'sample, fastq_1, fastq_2\n'
    csv_content = ''
    for sample_content in sample_list:
        sample_name = sample_content['sampleName']
        fastq_1     = sample_content['read1']
        fastq_2     = sample_content['read2']
        csv_content += '{},{},{}\n'.format(sample_name, fastq_1, fastq_2)
    with open(csv_file, 'w') as csv_f:
        csv_f.write(csv_header)
        csv_f.write(csv_content[:-1])


def steward(config_file_path:str, metatdenovo_path:str, send_message_script:str) -> None:
    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    analysis_path = os.path.dirname(config_file_path)
    os.chdir(analysis_path)
    # get the parameters from the config.json file
    with open(config_file_path, 'r') as config_f:
        config_file_d = json.load(config_f)
    # parameters got from the config.json file
    task_id            = config_file_d['taskId']
    analysis_record_id = config_file_d['analysisRecordId']
    task_name          = config_file_d['taskName']
    pipeline_name      = config_file_d['pipeline']
    patient_name       = config_file_d['patientId2']
    assembler          = config_file_d['parameterList']['assembler']
    min_contig_length  = int(config_file_d['parameterList']['min_contig_length'])
    orf_caller         = config_file_d['parameterList']['orf_caller']
    if config_file_d['parameterList']['skip_qc'] == 'false':
        skip_qc = False
    else:
        skip_qc = True
    if config_file_d['parameterList']['skip_fastqc'] == 'false':
        skip_fastqc = False
    else:
        skip_fastqc = True
    if config_file_d['parameterList']['skip_trimming'] == 'false':
        skip_trimming = False
    else:
        skip_trimming = True
    if config_file_d['parameterList']['save_trimmed'] == 'false':
        save_trimmed = False
    else:
        save_trimmed = True
    if config_file_d['parameterList']['save_bam'] == 'false':
        save_bam = False
    else:
        save_bam = True
    if config_file_d['parameterList']['save_samtools'] == 'false':
        save_samtools = False
    else:
        save_samtools = True

    # to generate the file: samplesheet.csv
    csv_file = '{}/samplesheet.csv'.format(analysis_path)
    make_csv_file(csv_file, config_file_d['taskSampleList'])

    # to generate the file: param.json
    script_dir = os.path.dirname(os.path.abspath(__file__))
    eggnog_dbpath = '{}/../eggnog'.format(script_dir)
    kofam_dir = '{}/../kofam'.format(script_dir)
    hmmfiles = '{0}/../hmmfiles/PF00317.hmm,{0}/../hmmfiles/PF00848.hmm,{0}/../hmmfiles/PF03477.hmm,{0}/../hmmfiles/PF13597.hmm'.format(script_dir)
    eukulele_dbpath = '{}/../gtdb_eukulele'.format(script_dir)
    params_d = {
        'input'            : csv_file,
        'outdir'           : 'results',
        'skip_qc'          : skip_qc,
        'skip_fastqc'      : skip_fastqc,
        'skip_trimming'    : skip_trimming,
        'save_trimmed'     : save_trimmed,
        'assembler'        : assembler,
        'min_contig_length': min_contig_length,
        'save_bam'         : save_bam,
        'save_samtools'    : save_samtools,
        'orf_caller'       : orf_caller,
        'skip_kofamscan'   : True,
        'eggnog_dbpath'    : eggnog_dbpath,
        'kofam_dir'        : kofam_dir,
        'hmmfiles'         : hmmfiles,
        'eukulele_dbpath'  : eukulele_dbpath
    }
    params_file = '{}/params.json'.format(analysis_path)
    with open(params_file, 'w') as params_f:
        json.dump(params_d, ensure_ascii=False, fp=params_f, indent=4)
    metatdenovo_command = 'nextflow run -offline -profile singularity -bg -params-file {} {} >>.nextflow_stdout.txt'.format(params_file, metatdenovo_path)
    return_value = os.system(metatdenovo_command)
    logging.info(metatdenovo_command)
    logging.info('return value:{}\n'.format(str(return_value)))

    # to monitor the pipeline execution status and send messages
    monitor_execution_params_d = {
        'analysis_path'      : analysis_path,
        'send_message_script': send_message_script,
        'return_value'       : return_value,
        'start_time'         : start_time,
        'task_id'            : task_id,
        'analysis_record_id' : analysis_record_id,
        'assembler'          : assembler,
        'orf_caller'         : orf_caller
    }
    monitor_execution(monitor_execution_params_d)
    logging.info('分析结束，开始整理分析报告')

    # to generate the report json file
    make_json_report_params_d = {
        'analysis_path'      : analysis_path,
        'send_message_script': send_message_script,
        'task_id'            : task_id,
        'analysis_record_id' : analysis_record_id,
        'assembler'          : assembler,
        'orf_caller'         : orf_caller
    }
    make_json_report(make_json_report_params_d)
    logging.info('分析报告整理完成')


def main() -> None:
    parser = argparse.ArgumentParser(description='transfer the config file to csv file, invoke the pipeline, feddback the information to front end')
    parser.add_argument('--cfg', required=True, help='the full path for the config.json file')
    parser.add_argument('--metatdenovo_path', required=True, help='the full path for metatdenovo')
    parser.add_argument('--send_message_script', required=True, help='the full path for the shell script: sendMessage.sh')
    args = parser.parse_args()
    config_file_path = args.cfg
    metatdenovo_path = args.metatdenovo_path
    send_message_script = args.send_message_script
    # logging
    log_file = '{}/metatdenovo.log'.format(os.path.dirname(config_file_path))
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    steward(config_file_path, metatdenovo_path, send_message_script)


if __name__ == '__main__':
    main()