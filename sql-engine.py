#!/usr/bin/env python
# coding: utf-8

# In[3]:


import sys
import os
import csv
import sqlparse
import pprint
import re
tablemeta= {}


# In[4]:


def readmetafile(file):
    f = open(file,'r')
    for x in f:
        if x.strip()=="<begin_table>":
            flag=1
        else:
            if flag==1:
                table= x.strip();
                tablemeta[table]=[]
                flag=0
            else:
                if x.strip()!="<end_table>":
                    tablemeta[table].append(x.strip())


# In[5]:


def readsingletabledata(file):
    data=[]
    filename = "files/"+file+".csv"     
    if os.path.exists(filename):
        f=open(filename,'r')
        table=csv.reader(f)
        for row in table:
            data.append(row)
        return data
    else:
        print("Table does not exist.")
        sys.exit()


# In[9]:


def readdata(tables):
    data=[]
    if len(tables)==1:
        data =  readsingletabledata(tables[0])
        headers = [tables[0] + "." + header for header in tablemeta[tables[0]]]
        return data, headers
    else:
        t1= readsingletabledata(tables[0])
        if(t1==-1):
            return -1
        headers=([tables[0] + "." + header for header in tablemeta[tables[0]]])
        for i in range(1,len(tables)):
            t2=readsingletabledata(tables[i])
            if (t2==-1):
                return -1
            headers+=([tables[i] + "." + header for header in tablemeta[tables[i]]])
            for t1row in t1:
                for t2row in t2:
                    data.append(t1row + t2row)
            t1=data
            
    return data,headers


# In[10]:


def processcol(cols):
    columns=[]
    tables=idlist[3].split(",")
    for colname in cols:
        if len(colname.split("."))==2:
            columns.append(colname)
            continue
        flag=0
        for tabname in tables:
            if colname in tablemeta[tabname]:
                flag+=1
                columns.append(tabname+"."+colname)
        if flag==0 :
            print("Atribute "+ colname + " does not exist.")
            sys.exit()
        if flag>1 :
            print("Ambiguous attribute: ",colname, ".")
            sys.exit()
    return columns


# In[25]:


def processcoldistinct(cols):
    columns=[]
    tables=idlist[4].split(",")
    for colname in cols:
        if len(colname.split("."))==2:
            columns.append(colname)
            continue
        flag=0
        for tabname in tables:
            if colname in tablemeta[tabname]:
                flag+=1
                columns.append(tabname+"."+colname)
        if flag==0 :
            print("Atribute "+ colname + " does not exist.")
            sys.exit()
        if flag>1 :
            print("Ambiguous attribute: ",colname, ".")
            sys.exit()
    return columns


# In[11]:


def findcolnumbers(columns):
    columnnumbers=[]
    tables=idlist[3].split(",")
    data,headers =  readdata(tables)
    columns = processcol(columns)
    if columns==-1:
        return -1
    for column in columns:
        columnnumber = headers.index(column)        
        columnnumbers.append(columnnumber)
    return columnnumbers


# In[12]:


def project(idlist):
    tables=idlist[3].split(",")
    columns = idlist[1].split(",")

    data,headers = readdata(tables)
    columns = processcol(columns)  
    if columns==-1:
        return -1
    columnnumbers=findcolnumbers(columns)
    
    j=0
    for i in range(len(headers)):
        if i in columnnumbers:
            if(j>0):
                print("," + headers[i], end="")
            else:
                print(headers[i],end='')
            j+=1
    print()
    
    for line in data:
        j=0
        row=[line[i] for i in columnnumbers]
        for element in row:
            if(j>0):
                print(",",element,end="")
            else:
                print(element,end='')
            j+=1
        print()
    return 0


# In[13]:


def operate(a,b, op):
    if op == "=":
        return (a==b)
    if op == ">":
        return (a>b)
    if op== "<":
        return (a<b)
    if op == ">=":
        return (a>=b)
    if op == "<=":
        return (a<=b)
    else:
        return False


# In[14]:


def iscolumn(colname,headers):
    if( colname.isdigit() or colname.startswith('-') and colname[1:].isdigit()):
        return -1;
 
    if len(colname.split("."))==1:
        name = processcol(list(colname))
        if name==-1:
            return name
        if name[0] in headers:
                return headers.index(name[0])
        else:
            return -1;
    
    else:
        if colname in headers:
            return headers.index(colname)
        else:
            return -1;      


# In[15]:


def printheaders(columns):
    i=0
    for column in columns:
        if i>0:
            print(", ",column,end='')
        else:
            print(column, end='')
        i+=1
    print()


# In[16]:


def singlewhere(where,tables):
    data,headers = readdata(tables)
    columns = idlist[1].split(",")
    columns= processcol(columns)
    if columns==-1:
        return -1
    if '*' in columns:
        columns = headers
        
    chunk=[]
    columnnumbers=findcolnumbers(columns)
    
    relationalop=[">=","<=",">","<","="]
    flag=0
    for op in relationalop:
        if op in where:
            flag=1
            break
    
    if flag==0:
        print("Please enter a valid relational operator for where.")
        sys.exit()
   
    whereparts=where.replace(";","").split(op)

    columnnumber1 = iscolumn(whereparts[0],headers)
    columnnumber2 = iscolumn(whereparts[1],headers)

    
    if columnnumber1==-1 and columnnumber2==-1:
        print("Please enter a valid where condition.")
        sys.exit()
    elif columnnumber1==-1:

        for line in data:
            if operate(int(line[columnnumber2]),int(whereparts[0]),op):
                row=[line[i] for i in columnnumbers]
                chunk.append(row)
    
    elif columnnumber2==-1:

        for line in data:
            if operate(int(line[columnnumber1]),int(whereparts[1]),op):
                row=[line[i] for i in columnnumbers]
                chunk.append(row)
    else:
        if "=" in where:
            if columns == headers:
                removecolumn = tables[1]+"." + whereparts[1].split(".")[1]
                columns.remove(removecolumn)
                columnnumbers = findcolnumbers(columns)
        for line in data:
            if operate(int(line[columnnumber1]),int(line[columnnumber2]),op):
                row=[line[i] for i in columnnumbers]
                chunk.append(row)
    
    return chunk


# In[17]:


def wherequery(idlist,tables):
    where = idlist[4].split()
    columns = idlist[1].split(",") 
    columns =processcol(columns)
    data,headers = readdata(tables)
    
    if '*' in columns:
        columns = headers
    columnnumbers=findcolnumbers(columns)

    if len(where)==2:
        chunk=singlewhere(where[1],tables)

    
    elif  len(where)==4:
        
        if 'AND' in where or 'and' in where: 

            chunk1=singlewhere(where[1],tables)
            chunk2=singlewhere(where[3],tables)
            chunk=[]

            for c1 in chunk1:
                for c2 in chunk2:
                    if c1==c2:
                        chunk.append(c1)
        elif 'OR' in where or 'or' in where:

            chunk1=singlewhere(where[1],tables)
            chunk2=singlewhere(where[3],tables)
            chunk=[]
            for c1 in chunk1:
                chunk.append(c1)
            for c2 in chunk2:
                if c2 not in chunk:
                    chunk.append(c2)
        else:
            print("Please enter a where condition with only AND or OR.")
            sys.exit()
    else:
        print("Please enter a correct where condition.")
        sys.exit()
    
    printheaders(columns)
    for line in chunk:
        i=0
        for x in line:
            if i>0:
                print(",",x,end='')
            else:
                print(x,end="")
            i+=1
        print()


# In[23]:


def distinctquery(idlist):
    idlist[2] = idlist[2].replace(" ","")
    columns = idlist[2].split(",")

    columnnumbers=[]
    idlist[4] = idlist[4].replace(" ","")
    tables = idlist[4].split(",")
    columns = processcoldistinct(columns)
   
    data,headers=readdata(tables)
    for column in columns:
        columnnumber = headers.index(column)        
        columnnumbers.append(columnnumber)
    

    myset={}
    finallist=[]
    for line in data:
        row=[line[i] for i in columnnumbers]
        finallist.append(tuple(row))
    data = list(set(finallist))    
    printheaders(columns)
    for line in data:
        i=0
        for x in line:
            if i>0:
                print(",",x, end="")
            else:
                print(x,end='')
            i+=1
        print()


# In[19]:


def executestar(idlist):
    tables=idlist[3].split(",")
    data,headers=readdata(tables)
    i=0
    for header in headers:
        if(i>0):
            print(",",header,end='')
        else:
            print(header,end='')
        i+=1;
    print()
    
    for row in data:
        i=0
        for x in row:
            if i>0:
                print("," , x ,end='')
            else:
                print(x,end='')
            i+=1
        print()


# In[20]:


def execute(query):
    quer=query.strip()
    if query[-1]!=';':
        print("; MISSING.")
        sys.exit()
    query=query.replace(";","")
    global idlist
    idlist=[]
#     try:
    parse = sqlparse.parse(query)[0].tokens
    qtype = sqlparse.sql.Statement(parse).get_type()
    l = sqlparse.sql.IdentifierList(parse).get_identifiers()

    for i in l:
        idlist.append(str(i))  

    if (qtype == 'SELECT'):
        if len(idlist) < 4:
            print("Invalid Query. Please type a correct query")
            sys.exit()
        if idlist[1].lower()=='distinct':
            distinctquery(idlist)
            return

        idlist[1] = idlist[1].replace(" ","")
        function = re.sub(r'[\(\)]',' ',idlist[1]).split()


        idlist[3] = idlist[3].replace(" ","")
        tables = idlist[3].split(",")


        if len(tables)<1:
            print("Please specify table name(s)")
            sys.exit() 

        data,headers=readdata(tables)
        function[0]=function[0].lower()

#         print("FUNCTION",function[0])   
        #select * from table1
        if function[0]=='*' and len(idlist)==4:
            executestar(idlist)


        elif function[0]=='max' or  function[0]=='min' or function[0]=='sum' or function[0]=='avg'or function[0]=='distinct':
            column=function[1]
            try:
                columnnumber = tablemeta[tables[0]].index(column)
                values=[]
                for row in data:
                    values.append(int(row[columnnumber]))
            except ValueError:
                print("Attribute not found:",column)
                sys.exit()

            if function[0]=='max':
                print("max(",tables[0],".",column,")")
                print(max(values))
            elif function[0]=='min':
                print( "min(",tables[0],".",column,")")
                minv=values[0]
                for i in range(1, len(values)):
                    if values[i]<minv:
                        minv=values[i]
                print(minv)
            elif function[0]=='sum':
                print( "sum(",tables[0],".",column,")")
                print(sum(values))
            elif function[0]=='avg':
                print( "avg(",tables[0], ".",column,")")
                print(sum(values)/len(values))
            elif function[0]=='distinct':
                uniques=list(set(values))
                for u in uniques:
                    print(u)

        else:  
            projectcol = re.sub(r'[\(\)]',' ',idlist[1]).split()
            #simple projection of columns with no where and no join
            if len(idlist)==4:
                    t= project(idlist)
            elif len(idlist)==5:
                wherequery(idlist,tables)
    else:
        print("Please enter a valid sql query")
        sys.exit()

#     except Exception as e:
#         print("EROOORRRR ",e)


# In[21]:


readmetafile('files/metadata.txt')
query = sys.argv[1]
if query == "exit":
    sys.exit()
else: 
    execute(query)


# In[ ]:




