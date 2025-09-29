'''
本文件中的一系列脚本，


'''
import sys
from pathlib import Path
import win32com

from core.engine import ProcessingContext
from decorators.processor import processor, pre_processor, post_processor
SCRIPT_DIR = Path(__file__).parent.resolve()   ##此脚本的路径

sys.path.append(r'Q:\ipython')
from read import *
from write1 import *
from gcc_origin import *

# 初始化，提供初始环境，context
@pre_processor(name = "init", source = SCRIPT_DIR)
def init(context: ProcessingContext, **kwargs):

    ##输出路径
    outdoc = kwargs.get("out_doc")

    #设置word
    word = win32com.client.Dispatch('Word.Application')
    print(f'word = {word}')
    doc = word.Documents[0]
    context.set_shared(['init','word'], word)
    context.set_shared(['init','doc'], doc)
    #设置origin
    origin = win32com.client.Dispatch('Origin.ApplicationSI')
    origin_root_name = ''
    origin.Execute("pe_cd " + origin_root_name)  # 在origin中建立目录
    context.set_shared(['init','origin'], origin)

    ## 载荷数据集合
    context.set_shared(["blade_load_data", "amp_mean" ], [] )
    context.set_shared(["blade_load_data", "col_name" ], ["桨向位置", "扭矩", "挥舞弯矩", "摆振弯矩", "轴力", "弦向力", "法向力"] )   # 各列数据名
    context.set_shared(["blade_load_data", "unit_name" ], ['m', 'Nm', 'Nm', 'Nm', 'N', 'N', 'N']  )     # 各列数据单位
    context.set_shared(["blade_load_data", "amp" ], []  )      # 幅值数据（数据名：二维表）
    context.set_shared(["blade_load_data", "mean" ], []  )     # 均值数据（数据名：二维表）


## 后处理，
# config指定2个变量
#  'out_csv_file':   所有载荷工况输出为csv名
#  'out_file'：  所有载荷工况输出为二进制文件名

@post_processor(name = "post", source = SCRIPT_DIR)
def post(context: ProcessingContext, **kwargs):
    ## 
    out_csv_file = kwargs.get('out_csv_file', 'out.csv')
    out_file = kwargs.get('out_file', 'out')
    amp_mean_list = context.get_shared(["blade_load_data", "amp_mean" ])      #所有工况载荷dataframe的列表  

    ##合并dataframe
    dff = pd.concat(amp_mean_list,ignore_index = False)   ##依据索引合并

    #pickle导出
 #   pickle_amp_file = Path(out_file)
    f1 = open(out_file, 'wb')
    pickle.dump(dff, f1)
    f1.close()
    
    ##csv文件导出
  #  csv_file = Path(out_csv_file)
    dff.to_csv(out_csv_file, encoding = 'utf-8-sig')




# 获取指定.out文件内的所有菜叶转速、最大、最小、平均、瞬值等
@processor(name="get_load_data", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "get_load_data",
    "author": "guancc",
    "version": "1.0",
    "description": "从.out文件中获取桨叶稳态载荷数据",
    "supported_types": [""],
    "tags": [""]
})
def get_load_data(path: Path, context, **kwargs):

    ncol = 6
    key_list = [['\s+ROTOR\s+(\d+)', '\s+RADIUS \(M\)\s+=\s+(\S+)' , '[\s\S]+ROTATIONAL SPEED \(RPM\)\s+=\s+(\S+)', '\s+(\S+)\s+ROTATION DIRECTION'],            ##旋翼旋向
               '\s+OPERATING CONDITION','\s+TRIM SOLUTION',
               [r'\s+OUTPUT = ROTOR (\d+) BLADE 2 LOAD (\d+\.\d+)R F ',
               r'\s+MEAN' + '\s+(\S+)'*ncol,
               r'\s+MAXIMUM' + '\s+(\S+)'*ncol,
               r'\s+MINIMUM' + '\s+(\S+)'*ncol,
               r'\s+1/2 PEAK-TO-PEAK' + '\s+(\S+)'*ncol,
               [r'\s+PSI\s+=' + '\s+(\S+)'*(ncol+1)],'\s*\n']   #'\s+===' 
               ]
    
    
    gcc_int = lambda x:int(float(x))
    
    data_type = [[[gcc_int], [float], [float], [str] ],
                 [], [],
                 [[gcc_int,  float],   #gcc_int,
                 [float]*ncol, [float]*ncol,[float]*ncol, [float]*ncol,
                 [[float]*(ncol + 1)],[]]
                ]

    try:
        f = open(path, 'r')
        ret = re0_readAndFind1(f, key_list, data_type=data_type)
        data = [dataij for dataij in ret[1]]  # [file]
        f.close()
        
#        print(f'.........get_load_data= {ret}')
        datainfo = data[0][0]  # 旋翼号、旋翼半径、旋翼转速、旋翼旋向
        datainfo_dict = {ai[0]: (ai[1], ai[2], ai[3]) for ai in datainfo}
        
        ## 工况名
        parent_path = path.parent
        filename = path.name
        case_name = context.get_data([ "file_ops", "path_name_dict", str(parent_path), filename],  str(path)) 
                
        # 输出数据
        context.set_shared(["get_load_data", "datai" ], data[0][1] )
        context.set_shared(["get_load_data", "case_name" ], case_name )
        context.set_shared(["get_load_data", "datainfo" ], datainfo_dict )

        return {
            "file": str(path),
            "processor": "get_load_data",
            "status": "succeed"
        }

    except Exception as e:
        return {
            "file": str(path),
            "processor": "get_load_data",
            "status": "error",
            "error": str(e)
        }



# 处理数据，得到一个工况所有旋翼桨叶的各剖面载荷静值和动值
@processor(name="process_load_data", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "process_load_data",
    "author": "guancc",
    "version": "1.0",
    "description": "处理提取的数据，得到一个工况所有旋翼桨叶的各剖面载荷静值和动值",
    "supported_types": [""],
    "tags": [""]
})
def process_load_data(path: Path, context, **kwargs):
    datai = context.get_shared(["get_load_data", "datai" ]) 

    ii = 0
    counter = []
    load_r_amp = []
    load_r_mean = []
    maxi = []
    mini = []
    ampi = []
    meani = []
    ri = []
    rb = []
    iblade = 2
    for j in range(len(datai)):
        irotor = datai[j][0]  # 旋翼号
#        iblade = datai[j][1]  # 桨叶号
        rj = datai[j][1]  # 占位

        if (irotor, iblade) not in rb:
            counter.append(ii) if j > 0 else None
            ii = 1
        else:
            ii += 1
        rb.append((irotor, iblade))

        ri.append(rj)
        kk = 2
        meanj = datai[j][kk:kk+6]
        maxj = datai[j][kk+6:kk+12]
        minj = datai[j][kk+12:kk+18]
        ampj = datai[j][kk+18:kk+24]
        loaddata = datai[j][kk+24]

        maxi.append(maxj)
        mini.append(minj)
        ampi.append(ampj)
        meani.append(meanj)

    load_r_amp = [[ri[k]] + ampi[k] for k in range(len(ri))]
    load_r_mean = [[ri[k]] + meani[k] for k in range(len(ri))]
    counter.append(ii)

  #  comments1 = ['*'] + [case_name] * 6
    nr = len(counter)
    for k in range(nr):
        k1 = sum(counter[:k])  # (旋翼号，桨叶号)循环
        k2 = sum(counter[:k+1])
        ir, ib = rb[k1]
    
        load_r_amp_k = load_r_amp[k1:k2]
        load_r_mean_k = load_r_mean[k1:k2]
    
        context.set_shared(["process_load_data", "load_r_amp", (ir, ib) ], load_r_amp_k)
        context.set_shared(["process_load_data", "load_r_mean", (ir, ib) ], load_r_mean_k)
    
    ##提取数据的列名
    context.set_shared(["process_load_data", "col_names"],  [
        '展向位置', '扭矩', '挥舞弯矩', '摆振弯矩', '轴力', '弦向力', '法向力'
    ])
    context.set_shared(["process_load_data", "col_units"], [
        'm', 'Nm', 'Nm', 'Nm', 'N', 'N', 'N'
    ])



@processor(name="write_path_to_word", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "write_path_to_word",
    "author": "guancc",
    "version": "1.0",
    "description": "将path的label写如word",
    "supported_types": [""],
    "tags": [""]
})
def write_path_to_word(path: Path, context: ProcessingContext, **kwargs):

   # 参数变量
    style = kwargs.get("style", '正文')
    ##context 变量
    word = context.get_shared(['init','word'] ) 
    doc = context.get_shared(['init','doc'] ) 

    ## 取得path的标签
    text = context.get_data(['labels', str(path)])[-1]
    #主体  
    Writetext2word(word, doc, text=text, style=style)


##将一个工况的载荷写入word
# 写3部分信息， 1）标题，工况名；2）文本，简单说明描述；3）载荷表格。
## kwargs可指定3个参数，  'style_case': 工况名的样式，如'标题 1'； 'style_part'： 工况一部分的样式，如'正文'; 'style_text':正文样式
@processor(name="write_load_to_word", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "write_load_to_word",
    "author": "guancc",
    "version": "1.0",
    "description": "将一个工况的载荷数据写入word",
    "supported_types": [""],
    "tags": [""]
})
def write_load_to_word(path: Path, context, **kwargs):
    ##从kwargs中获取参数
    style_case = kwargs.get('style_case', '正文')
    style_part = kwargs.get('style_part', '正文')
    style_text = kwargs.get('style_text', '正文')
    ##从context中获取参数
    word = context.get_shared(["init", "word" ])  
    doc = context.get_shared(["init", "doc" ])  
    amp_ir_ib_dict = context.get_shared(["process_load_data", "load_r_amp" ])   #  shared['process_load_data']['load_r_amp']
    col_names = context.get_shared(["process_load_data", "col_names" ])   # shared['process_load_data']['col_names']
    col_units = context.get_shared(["process_load_data", "col_units" ])   # shared['process_load_data']['col_units']
    header = ['展向向位置(1/R)'] + [f'{col_names[i]}({col_units[i]})' for i in range(1, 7)]
   
    
    ## 工况名
#    parent_path = path.parent
#    filename = path.name
#    text = context.get_data([ "file_ops", "path_name_dict", str(parent_path), filename],  str(path)) 
    text = context.get_shared(["get_load_data", "case_name" ] )
    Writetext2word(word, doc, text=text, style=style_case)    
    
    for (ir, ib), amp_data in amp_ir_ib_dict.items():

        ####### 写入word #############
        text = '旋翼' + str(ir) + '载荷'
        Writetext2word(word, doc, text=text, style = style_part)  # ZHZ 3级标题[0]
        text = '此工况下' + '旋翼' + str(ir) + '号' + '桨叶剖面的载荷' + '如下表所示。'  # 粘贴图，随展向剖面位置变化
        Writetext2word(word, doc, text=text, style=style_text)  # ZHZ正文

        amp_data1 = [[round(dataij, 2) for dataij in datai] for datai in amp_data]
        out_data = [header] + amp_data1
        labelname = '桨叶各剖面载荷'
        Writedata2word(word, doc, out_data, labelname=labelname)





#  进入origin中的文件夹，若不存在，则在origin中创建文件夹，并进入，一般处理文件夹
## origin中的数据结构与 root下的文件路径一致。一般和set_path_name_dict结合使用
@processor(name="origin_cd", priority=60, source = SCRIPT_DIR, type_hint="path", metadata={
    "name": "origin_cd",
    "author": "guancc",
    "version": "1.0",
    "description": "origin中，进入path对应的文件夹，若没有则创建",
    "supported_types": [""],
    "tags": [""]
})
def origin_cd(path: Path, context: ProcessingContext, **kwargs):
    if not path.is_dir():
        return

    origin = context.get_shared(['init', 'origin'])
    ##
    root = context.root_path
    rel_path = rel_path = path.relative_to(root)
    parts = list(rel_path.parts)   #默认
    labels = context.get_data(['labels',  str(path)], parts)
    
    ##先回到最顶层
    not_done = True
    while not_done:
        not_done = origin.Execute('pe_cd .. ')
        
    #再从最顶层向内层进入
    for pi in labels:
        done = origin.Execute('pe_cd ' + pi)
        if not done:
            origin.Execute('pe_mkdir ' + pi)
            origin.Execute('pe_cd ' + pi)
            

##将一个工况的载荷数据写入origin
## 
@processor(name="write_load_to_origin", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "write_load_to_origin",
    "author": "guancc",
    "version": "1.0",
    "description": "将一个工况的载荷数据写入origin，并作图",
    "supported_types": [""],
    "tags": [""]
})
def write_load_to_origin(path: Path, context, **kwargs):
    
    ##从context中获取数据
    amp_ir_ib_dict = context.get_shared(["process_load_data", "load_r_amp" ]) 
    mean_ir_ib_dict = context.get_shared(["process_load_data", "load_r_mean" ]) 
    col_names = context.get_shared(["process_load_data", "col_names" ])
    col_units = context.get_shared(["process_load_data", "col_units" ])
    #case_name = context.get_shared(["process_load_data", "case_name" ]) 
    case_name = context.get_shared(["get_load_data", "case_name" ] )
     #
    called_path = write_load_to_origin.called_path
    called_path.append(path)      ##收集调用此函数的路径     
    case_list =  [pi for pi in called_path if pi.parent == path.parent]   ## 与path相同父目录的文件集合
    i = len(case_list)

    ##  
    labels = context.get_data(['labels', str(path)])
    pre_fix0 = '_'.join(labels[:-1])      # '_'.join(case_name)  # 作图的前缀

    for (ir, ib), amp_data in amp_ir_ib_dict.items():
        mean_data = mean_ir_ib_dict[(ir, ib)]

        pre_fix = 'r' + str(ir) + 'b' + str(ib)  # 旋翼x桨叶
        bookname =  pre_fix0 + '_' + case_name  # origin中book名
        sheetname = pre_fix + '_各剖面动载'  # 各剖面动载

        gcc_CreateBookSheet(amp_data, bookname=bookname, sheetname=sheetname,
                            booknametype=0,
                            name=col_names, comments=['*'] + [case_name] * 6, units=col_units, Ismod_bn=1)
        
        # 静载和动载曲线展向位置变化
        graphnames = [pre_fix0 + '_' + pre_fix + '_' + gi for gi in col_names[1:]]
        ri_graph_obj_list = []
        for p in range(6):  # origin中画图, 6张graph中分别画图
            retc = gcc_CreatePlotinOrigin1(bookname, sheetname, [p+2], graphname=graphnames[p], Ismod_gh=1,
                                        lineopt_list=[{'-c': i, '-w': 800, '-k': i, '-kf': 1, '-z': 7}])
            ri_graph_obj_list.append(retc)



import pandas as pd
##将单个工况中，所有桨叶的载荷数据加入dataframe
@processor(name="add_load_to_dataframe", priority=60, source = SCRIPT_DIR, type_hint="file", metadata={
    "name": "add_load_to_dataframe",
    "author": "guancc",
    "version": "1.0",
    "description": "将单个工况中，所有桨叶的载荷数据加入dataframe",
    "supported_types": [""],
    "tags": [""]
})
def add_load_to_dataframe(path: Path, context, **kwargs):

    ##从context中获取
    data = context.get_shared(["get_load_data", "datai" ])
    datainfo_dict = context.get_shared(["get_load_data", "datainfo" ])
    case_labels = context.get_data(['labels', str(path)])   ## 描述工况的列表
    col_names = context.get_shared(["process_load_data", "col_names" ])

    ## path的分类名和label
    root = context.root_path
    rel_path = rel_path = path.relative_to(root)
    parts = list(rel_path.parts)   #默认
    labels = context.get_data(['labels', str(path)], parts)    ##若没使用set_path_name_dict, 则默认为各级文件夹名
    categories = context.get_data(['categories', str(path)], [i for i in range(len(parts))])   ##若没使用set_path_name_dict, 则默认为[1,2,3,4...]

    load_amp_mean = []  # 收集动载和静载

    for j in range(len(data)):  # 展向占位循环 在origin中收集数据并作图
        irotor = data[j][0]  # 旋翼号
        iblade = data[j][1]  # 桨叶号
        rj = data[j][2]  # 占位

        radiusj = datainfo_dict[irotor][0]
        rpmj = datainfo_dict[irotor][1]
        rotatej = datainfo_dict[irotor][2]

        meanj = labels + [irotor, iblade, radiusj, rpmj, rotatej, '静载', rj] + data[j][3:9]
        ampj = labels + [irotor, iblade, radiusj, rpmj, rotatej, '动载', rj] + data[j][21:27]

        load_amp_mean.append(ampj)
        load_amp_mean.append(meanj)

    columns_name = categories + ['旋翼号', '桨叶号', '展向半径', '旋翼转速', '旋翼旋转', '载荷类型'] + col_names  #
    load_index_name =  categories + ['旋翼号', '桨叶号', '展向位置', '载荷类型']

    load_amp_mean_df = pd.DataFrame(load_amp_mean, columns=columns_name)
    load_amp_mean_df1 = load_amp_mean_df.set_index(load_index_name)

    ##收集载荷数据
    amp_mean = context.get_shared(["blade_load_data", "amp_mean" ] )
    amp_mean.append(load_amp_mean_df1)



