#!/usr/bin/env python3

import os
import logging
from metatdenovo_funcs import send_json_message

def make_json_report(make_json_report_params_d: dict):
    analysis_path       = make_json_report_params_d['analysis_path']
    send_message_script = make_json_report_params_d['send_message_script']
    task_id             = make_json_report_params_d['task_id']
    analysis_record_id  = make_json_report_params_d['analysis_record_id']
    assembler           = make_json_report_params_d['assembler']
    orf_caller          = make_json_report_params_d['orf_caller']
    logging.info(make_json_report_params_d)
    logging.info(analysis_path)
    os.chdir(analysis_path)
    # to generate the report to display on front end
    report_dict = {
        'status'          : 'Pass',
        'pipelineName'    : 'metatdenovo'.upper(),
        'taskId'          : task_id,
        'analysisRecordId': analysis_record_id,
        'error'           : 0,
        'taskName'        : 'Report',
        'data'            : []
    }
    # 测试数据基本信息
    report_seq_stat_dict = {
        'sort'     : 1,
        'title'    : '测序数据信息',
        'subtitle1': '测序数据质量评估',
        'memo'     : '',
        'table'    : {
            'data': [
                {
                    'content': '',
                    'path'   : '{}/results/multiqc/multiqc_data/mqc_fastqc_sequence_counts_plot_1.txt'.format(analysis_path),
                    'memo'   : '',
                    'preDes' : '使用FastQC软件, 对测序数据进行统计',
                    'title'  : '测序数据统计表',
                    'postDes': ''
                }
            ]
        }, 
        'image': {
            'data': [
                {
                    'title'  : '样本的数据量统计图',
                    'postDes': '其中Duplicate readsd的数量通过软件估计',
                    'memo'   : '',
                    'preDes' : '',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_fastqc_sequence_counts_plot_1.png'.format(analysis_path)
                },
                {
                    'title'  : '样本的数据量统计图(百分比)',
                    'postDes': '其中Duplicate readsd的数量通过软件估计',
                    'memo'   : '',
                    'preDes' : '',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_fastqc_sequence_counts_plot_1_pc.png'.format(analysis_path)
                }
            ]
        },
        'subtitle2': ''
    }
    report_dict['data'].append(report_seq_stat_dict)
    # 数据质控
    report_seq_qc_dict = {
        'sort'  : 2,
        'title' : '数据质量',
        'memo'  : '',
        'preDes': '数据预处理结果展示',
        'image' : {
            'data': [
                {
                    'sort'   : 1,
                    'title'  : '',
                    'postDes': 'reads中每个碱基的平均质量',
                    'memo'   : '',
                    'preDes' : 'Reads的每个碱基平均质量图',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_fastqc_per_base_sequence_quality_plot_1.png'.format(analysis_path)
                }, 
                {
                    'sort'   : 2,
                    'title'  : '',
                    'postDes': '每个Reads的平均质量分布',
                    'memo'   : '',
                    'preDes' : 'Reads的质量值',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_fastqc_per_sequence_quality_scores_plot_1.png'.format(analysis_path)
                }, 
                {
                    'sort'   : 3,
                    'title'  : '',
                    'postDes': '',
                    'memo'   : '每个Reads的平均GC含量',
                    'preDes' : 'Reads的GC含量图',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_fastqc_per_sequence_gc_content_plot_Counts.png'.format(analysis_path)
                },
                {
                    'sort'   : 4,
                    'title'  : '',
                    'postDes': '',
                    'memo'   : '每个Reads的长度分布',
                    'preDes' : 'Reads的长度分布图',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_fastqc_sequence_length_distribution_plot_1.png'.format(analysis_path)
                }
            ]
        }, 
        'subtitle2': ''
    }
    report_dict['data'].append(report_seq_qc_dict)
    # 转录本拼接+比对
    if assembler == 'megahit':
        assembly_res = '{}/results/megahit/megahit_out/megahit_assembly.contigs.fa.gz'.format(analysis_path)
        assembly_res_stat = '{}/results/multiqc/multiqc_data/multiqc_megahit_assemblies-plot.txt'.format(analysis_path)
    if assembler == 'rnaspades':
        assembly_res = '{}/results/rnaspades/rnaspades.transcripts.fa.gz'.format(analysis_path)
        assembly_res_stat ='{}/results/multiqc/multiqc_data/multiqc_rnaspades_assemblies-plot.txt'.format(analysis_path)
    tmp_file = '{}/results/multiqc/multiqc_data/multiqc_megahit_assemblies-plot_display.txt'.format(analysis_path)
    with open(assembly_res_stat, 'r') as infile, open(tmp_file, 'w') as outfile:
        for line in infile:
            fields = line.strip().split('\t')[1:-9]
            new_line = '\t'.join(fields) + '\n'
            outfile.write(new_line)
    assembly_res_stat = tmp_file
    del tmp_file
    report_assemble_align_dict = {
        'sort'     : 3,
        'title'    : '转录本拼接+比对',
        'subtitle1': '转录本拼接结果',
        'memo'     : '',
        'table'    : {
            'sort': 1,
            'data': [
                {
                    'content': '',
                    'path'   : assembly_res_stat,
                    'memo'   : '',
                    'preDes' : '使用{}拼接转录本'.format(assembler),
                    'title'  : '转录本拼接结果统计表',
                    'postDes': ''
                }
            ]
        },
        'image': {
            'data': [
                {
                    'sort'   : 1,
                    'title'  : ' reads比对统计图',
                    'postDes': '',
                    'memo'   : '',
                    'preDes' : '使用featureCounts统计reads比对到contigs的统计结果',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_featureCounts_assignment_plot_1.png'.format(analysis_path)
                },
                {
                    'sort'   : 2,
                    'title'  : 'reads比对统计图(百分比)',
                    'postDes': '',
                    'memo'   : '',
                    'preDes' : '使用featureCounts统计reads比对到contigs的统计结果（百分比展示）',
                    'path'   : '{}/results/multiqc/multiqc_plots/png/mqc_featureCounts_assignment_plot_1_pc.png'.format(analysis_path)
                }
            ]
        },
        'text': [
            {
                'sort'   : 1,
                'title'  : '拼接转录本结果',
                'content': '使用{}获得的转录本文件下载：#&{}'.format(assembler, assembly_res),
                'postDes': '下载后请解压，推荐使用记事本查看。',
                'memo'   : '',
                'preDes' : ''
            }
        ],
        'subtitle2': ''
    }
    report_dict['data'].append(report_assemble_align_dict)
    # ORF 检测
    if orf_caller == 'prodigal':
        nucleotides_fasta_file ='{}/results/prodigal/{}.prodigal.fna.gz'.format(analysis_path, assembler)
        amino_acides_fasta_file = '{}/results/prodigal/{}.prodigal.faa.gz'.format(analysis_path, assembler)
        genome_feature_file = '{}/results/prodigal/{}.prodigal.gff.gz'.format(analysis_path, assembler)
    if orf_caller == 'prokka':
        nucleotides_fasta_file = '{}/results/prodigal/prokka.ffn.gz'.format(analysis_path)
        amino_acides_fasta_file = '{}/results/prodigal/prokka.faa.gz'.format(analysis_path)
        genome_feature_file = '{}/results/prodigal/prokka.gff.gz'.format(analysis_path)
    if orf_caller == 'transdecoder':
        nucleotides_fasta_file = '{}/results/prodigal/{}_assembly.contigs.fa.transdecoder.cds'.format(analysis_path, assembler)
        amino_acides_fasta_file = '{}/results/prodigal/{}_assembly.contigs.fa.transdecoder.pep'.format(analysis_path, assembler)
        genome_feature_file = '{}/results/prodigal/{}_assembly.contigs.fa.transdecoder.gff3'.format(analysis_path, assembler)
    report_orf_caller = {
        'sort'     : 4,
        'title'    : 'ORF检测',
        'subtitle1': '',
        'subtitle2': '使用{}检测ORF结果，结果文件包含核苷酸序列文件(fasta格式)、氨基酸序列文件(fasta格式)、及基因组功能组件的注释文件(gff格式)'.format(orf_caller),
        'text'     : [
            {
                'sort'   : 1,
                'title'  : '核苷酸序列文件',
                'content': '核苷酸序列文件下载：#&{}'.format(nucleotides_fasta_file),
                'postDes': '',
                'memo'   : '下载后请解压，推荐使用记事本查看。',
                'preDes' : ''
            },
            {
                'sort'   : 2,
                'title'  : '氨基酸序列文件',
                'content': '氨基酸序列文件下载：#&{}'.format(amino_acides_fasta_file),
                'postDes': '',
                'memo'   : '下载后请解压，推荐使用记事本查看。',
                'preDes' : ''
            },
            {
                'sort'   : 3,
                'title'  : '基因组功能组件文件',
                'content': '基因组功能组件文件下载：#&{}'.format(genome_feature_file),
                'postDes': '',
                'memo'   : '下载后请解压，推荐使用记事本查看。',
                'preDes' : ''
            },
            {
                'sort'   : 4,
                'title'  : 'ORF表达定量结果',
                'content': 'ORF表达定量结果文件下载：#&{}/results/summary_tables/{}.{}.counts.tsv.gz'.format(analysis_path, assembler, orf_caller),
                'postDes': '',
                'memo'   : '通过{}构建转录本，通过{}进行ORF检测，然后使用BBmap将clean reads比对到转录本上进行ORF表达定量, 使用featureCounts计算表达量\n下载后请先解压，推荐使用Excel查看。'.format(assembler, orf_caller),
                'preDes' : ''
            }
        ]
    }
    report_dict['data'].append(report_orf_caller)
    # 功能和分类注释
    if orf_caller == 'prokka':
        tmp_prokka = '、Prokka'
    else:
        tmp_prokka = ''
    report_annotation = {
        'sort'     : 5,
        'title'    : '功能和分类注释',
        'subtitle1': '',
        'subtitle2': '分别使用Eggnog、HMMERSearch{}对ORF进行功能注释；使用EUKulele进行分类注释。'.format(tmp_prokka),
        'text'     : [
            {
                'sort'   : 1,
                'title'  : 'Eggnog功能注释结果',
                'content': 'Eggnog功能注释结果文件下载：#&{}/results/summary_tables/{}.{}.emapper.tsv.gz'.format(analysis_path, assembler, orf_caller),
                'postDes': '',
                'memo'   : '下载后请先解压，推荐使用Excel查看。',
                'preDes' : ''
            },
            {
                'sort'   : 2,
                'title'  : 'HMMERSearch功能注释结果',
                'content': 'HMMERSearch功能注释结果文件下载：#&{}/results/summary_tables/{}.{}.hmmrank.tsv.gz'.format(analysis_path, assembler, orf_caller),
                'postDes': '',
                'memo'   : '下载后请先解压，推荐使用Excel查看。',
                'preDes' : ''
            },
            {
                'sort'   : 3,
                'title'  : 'Prokka功能注释结果',
                'content': 'Prokka功能注释结果文件下载：#&{}/results/summary_tables/{}.{}.prokka-annotations.tsv.gz'.format(analysis_path, assembler, orf_caller),
                'postDes': '',
                'memo'   : '下载后请先解压，推荐使用Excel查看。',
                'preDes' : ''
            },
            {
                'sort'   : 4,
                'title'  : 'EUKulele分类注释结果',
                'content': 'EUKulele分类注释结果文件下载：#&{}/results/summary_tables/{}.{}.userdb_taxonomy.tsv.gz'.format(analysis_path, assembler, orf_caller),
                'postDes': '',
                'memo'   : '下载后请先解压，推荐使用Excel查看。',
                'preDes' : ''
            }
        ]
    }
    if orf_caller == 'prokka':
        pass
    else:
        report_annotation['text'][3]['sort'] = 3
        del report_annotation['text'][2]
    report_dict['data'].append(report_annotation)
    # multiqc report to download
    report_multiqc = {
        'sort'     : 6,
        'title'    : 'MultiQC报告',
        'memo'     : '',
        'subtitle1': 'MultiQC报告下载',
        'subtitle2': '',
        'text'     : [
            {
                'sort'   : 1,
                'title'  : 'MultiQC报告下载',
                'content': 'MultiQC下载：#&{}/results/multiqc/multiqc_report.html'.format(analysis_path),
                'postDes': '',
                'memo'   : '使用MultiQC将分析结果汇总到一个报告中，可使用浏览器查看该报告。',
                'preDes' : ''
            }
        ]
    }
    report_dict['data'].append(report_multiqc)
    send_json_message(analysis_path, send_message_script, report_dict, 'Report.json')
    # multiqc report to download
    report_multiqc = {
        'sort'     : 7,
        'title'    : 'MultiQC报告',
        'memo'     : '',
        'subtitle1': 'MultiQC报告下载',
        'subtitle2': '',
        'text'     : [
            {
                'sort'   : 1,
                'title'  : 'MultiQC报告下载',
                'content': 'MultiQC下载：#&{}/results/multiqc/multiqc_report.html'.format(analysis_path),
                'postDes': '',
                'memo'   : '使用MultiQC将分析结果汇总到一个报告中，可使用浏览器查看该报告。',
                'preDes' : ''
            }
        ]
    }
    report_dict['data'].append(report_multiqc)
    send_json_message(analysis_path, send_message_script, report_dict, 'Report.json')
