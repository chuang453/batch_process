import win32com.client
import os
import re
'''
将data写入tablefile中
method： 写出方式  1-无格式输出   2-格式化输出，格式为formats  其他--不写出文档，但按formats生成字符串 
formats：仅method为2时有用，用以设置格式。
      %个数一般与当前要格式化的数据长度相同
      若%数多余数据长度，则多出的格式部分部分忽略
      若%数小于数据长度，则多出的数据将会采用最后一个%格式
data： list   ,list的list
tablefile：写出的文件名。
返回值
data2 ：仅method不为1和2时有用，输出
'''
#word=win32com.client.Dispatch('Word.Application')
#selection=word.Selection

def Write2Table(data,tablefile,method=1,formats='%12.5f'):
    if method in [1,2]:
        f4=open(tablefile,'w')
    
    if method == 1:
        data_str1=[str(a) for a in data]
        data_str2=[a[1:-1].replace(',','\t')+'\n' for a in data_str1]
        for line in data_str2:
            f4.write(line)
        f4.close()
    else:
        n1 = len(re.findall('%',formats))
        ss=''
        data2=[]
        for datai in data:
            n2=len(datai)
            if n1>n2:
                a=formats.split('%')
                formats1='%'.join(a[:n2+1])
            else:
                i1=formats.rfind('%')
                formats1=formats[0:i1] + formats[i1:]*(n2-n1+1)
            ss=ss+formats1 % tuple(datai) + '/r/n'
            formats2=formats1.split('%')
#            print(datai)
#            print(formats2)
            dataj=[('%'+formats2[i+1]) % (datai[i]) for i in range(n2)]
            data2.append(dataj)
        if method == 2:
            f4.write(ss)
            f4.close()
        return data2


'''
在word文档中输入字符串text
word=win32com.client.Dispatch('Word.Application')
doc=word.Documents.Open(file)
file
'''
def Writetext2word(word,doc,text='',style='正文'):
  #  file=workdir+'\\'+filename
 #   word=win32com.client.Dispatch('Word.Application')
#    if filename not in os.listdir(workdir):
#        doc=word.Documents.Add()
#        doc.SaveAs(file)
#    else:
#        doc=word.Documents.Open(file)
    #word.Visible=1
    
    selection=word.Selection
    selection.EndKey(Unit=6)
#    selection.ParagraphFormat.Alignment=1    ##对齐方式，1 居中  3左对齐   2 右对齐
    selection.Style = doc.Styles(style) 
#    print(selection.ParagraphFormat.Alignment)
    selection.TypeText(Text=text)
    
    selection.TypeParagraph()
    selection.EndKey(Unit=6)
#    doc.Save()
#    if isclose:
#        doc.Close()    

#workdir,filename,isclose=0
##word=win32com.client.Dispatch('Word.Application') 为当前绑定的word application

def Paste2Word(word,label='图',labelname='我问问'):
#
#    word=win32com.client.Dispatch('Word.Application')
#    word.Visible=1
#    file=workdir+'\\'+filename
#  #  word=win32com.client.Dispatch('Word.Application')
#    print(word)
#    if filename not in os.listdir(workdir):
#        doc=word.Documents.Add()
#        doc.SaveAs(file)
#    else:
#        doc=word.Documents.Open(file)    
  #  doc=word.Documents.Open(FileName=file,Encoding='gbk')
#    print(word1)
    
    selection=word.Selection
    selection.EndKey(Unit=6)
    selection.Paste()
    selection.TypeParagraph()
    selection.ParagraphFormat.Alignment=1
    selection.InsertCaption(Label=label)
    selection.TypeText(Text=' '+labelname)
    
    selection.TypeParagraph()
    selection.EndKey(Unit=6)
    
#    doc.Save()
#    if isclose:
#        doc.Close()    
#        
'''
在word中插入对象
图片 、对象等，可通过word宏录制得到相关函数 
如插入origin graph对象 
Selection.InlineShapes.AddOLEObject(ClassType="Origin50.Graph", FileName="", LinkToFile=False, DisplayAsIcon=False
插入图片 
Selection.InlineShapes.AddPicture(FileName= "",LinkToFile=False, SaveWithDocument=True)

workdir: word文件所在目录
docfile： 操作得word对象
objfile：   所要插入的对象路径
objtype： 对象类型， 默认为origin graph对象，即"Origin50.Graph"  
labelname：图片label名
返回插入的对象 ob
通过ob可设置对象的尺寸大小 
如ob.Height =   设置高度
ob.Width =     设置宽度 
'''
##workdir,docfile,isclose=0

def gcc_insertobj(word,objfile,objtype="Origin50.Graph",labelname='我问问'):
#
#    word=win32com.client.Dispatch('Word.Application')
#    word.Visible=1
#    file=workdir+'\\'+docfile
#    word=win32com.client.Dispatch('Word.Application')
#    print(word)
#    if docfile not in os.listdir(workdir):
#        doc=word.Documents.Add()
#        doc.SaveAs(file)
#    else:
#        doc=word.Documents.Open(file)    
  #  doc=word.Documents.Open(FileName=file,Encoding='gbk')
#    print(word1)
    
    selection=word.Selection
    selection.EndKey(Unit=6)
    
    ob=selection.InlineShapes.AddOLEObject(ClassType=objtype, FileName=objfile, LinkToFile=False, DisplayAsIcon=False)

    selection.TypeParagraph()
    selection.ParagraphFormat.Alignment=1
    selection.InsertCaption(Label='图')
    selection.TypeText(Text=' '+labelname)
    
    selection.TypeParagraph()
    selection.EndKey(Unit=6)
#    doc.Save()
#    if isclose:
#        doc.Close()
    return ob

'''
插入图片 
Selection.InlineShapes.AddPicture(FileName= "",LinkToFile=False, SaveWithDocument=True)

返回插入的对象 
通过ob可设置对象的尺寸大小 
如ob.Height =   设置高度   如 6*28.35 代表6cm 
ob.Width =     设置宽度 
'''
##workdir,docfile,isclose=0

def gcc_insertpic(word,picfile,labelname='我问问'):
#
#    word=win32com.client.Dispatch('Word.Application')
#    word.Visible=1
#    file=workdir+'\\'+docfile
#    word=win32com.client.Dispatch('Word.Application')
#    print(word)
#    if docfile not in os.listdir(workdir):
#        doc=word.Documents.Add()
#        doc.SaveAs(file)
#    else:
#        doc=word.Documents.Open(file)    
  #  doc=word.Documents.Open(FileName=file,Encoding='gbk')
#    print(word1)
    
    selection=word.Selection
    selection.EndKey(Unit=6)
    
    ob=selection.InlineShapes.AddPicture(FileName=picfile,LinkToFile=False, SaveWithDocument=True)

    selection.TypeParagraph()
    selection.ParagraphFormat.Alignment=1
    selection.InsertCaption(Label='图')
    selection.TypeText(Text=' '+labelname)
    
    selection.TypeParagraph()
    selection.EndKey(Unit=6)
#    doc.Save()
#    if isclose:
#        doc.Close()
    return ob

   
    
##复制opj中的graphname到剪贴板   opj, 
def gcc_CopyPage(graphname):
    origin=win32com.client.Dispatch('Origin.ApplicationSI')
   # origin=origin0.Load(opjfile)
    origin.Copypage(graphname,4)


'''
在word中输添加一个表格table，其数据由data完全填满。
workdir: 要写入的word文件所在路径
filename：要写入的word名，从光标处向后继续填写
data： list  二维数据list，完全填满表格  如 [[1,2,3,'',2,'ewra'],[3,'','','34']]
datastyle: 表中文字的样式名 
labelname：表名
merge_list： list  对表格中某些单元格的合并操作，
       如：
       [[(1,2),(1,3),(1,4)],[(2,2),(2,3),(2,4)],[(1,2),(2,2)]]
       代表将 第1行2列、第1行3列、第1行4列单元格合并，
          再将第2行2列、第2行3列、第2行4列单元格合并
          最后再将第1行2列、第2行2列合并
          这里的行和列编号从1开始编号。 
'''
def Writedata2word(workdir,filename,data,datastyle="正文",labelname='表不不不',isclose=0, 
                   merge_list = []):
 #   workdir=r'D:\\'
 #   filename=r'ggg.doc'

    file=workdir+'\\'+filename
    word=win32com.client.Dispatch('Word.Application')
    if filename not in os.listdir(workdir):
        doc=word.Documents.Add()
        doc.SaveAs(file)
    else:
        doc=word.Documents.Open(file)
    #word.Visible=1
    selection=word.Selection
    selection.EndKey(Unit=6)
    ncol=len(data[0])
    nrow=len(data)
    selection.Style = doc.Styles(datastyle) 
    dti=doc.Tables.Add(selection.Range,nrow,ncol)
    
    i=1
    #dti=doc.Tables.Item(i)   #第i个表格对象
    dti.Borders.OutsideLineStyle=1;   #最外框，实线
    #dti.Borders.OutsideLineWidth=1;    #线宽 
    dti.Borders.InsideLineStyle=1;   #内框，实线
    #dti.Borders.InsideLineWidth=1;    #线宽
    ##定义行高、列宽
    selection.InsertCaption(Label='表')
    selection.Style = doc.Styles("题注")
    selection.TypeText(Text=' '+labelname) 
    selection.ParagraphFormat.Alignment=1    
    for i in range(nrow):
        for j in range(ncol):
#            print(data[i][j])
            dti.Cell(i+1,j+1).Range.Text=data[i][j]
            
#    for i in range(nrow):
#        selection.MoveDown() 
    selection.MoveDown() 
    selection.EndKey(Unit=6)
    selection.TypeParagraph()
    
    for mergei in merge_list:
        [dti.Cell(mergei[0][0],mergei[0][1]).Merge(dti.Cell(xi[0],xi[1])) for xi in mergei[1:]]
    
    
    doc.Save()
    if isclose:
        doc.Close()   
    
    return dti


'''
在word文档中输入字符串text
workdir: 要写入的word文件所在路径
filename：要写入的word名，从光标处向后继续填写
file
'''
def writetext2word1(workdir,filename,text='',style='正文',isclose=0):
    file=workdir+'\\'+filename
    word=win32com.client.Dispatch('Word.Application')
    if filename not in os.listdir(workdir):
        doc=word.Documents.Add()
        doc.SaveAs(file)
    else:
        doc=word.Documents.Open(file)
    #word.Visible=1
    
    selection=word.Selection
    selection.EndKey(Unit=6)
#    selection.ParagraphFormat.Alignment=1    ##对齐方式，1 居中  3左对齐   2 右对齐
    selection.Style = doc.Styles(style) 
#    print(selection.ParagraphFormat.Alignment)
    selection.TypeText(Text=text)
    
    selection.TypeParagraph()
    selection.EndKey(Unit=6)
    doc.Save()
    if isclose:
        doc.Close()    

