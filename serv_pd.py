from datetime import datetime, timedelta
from collections import OrderedDict
import pandas as pd
import sqlalchemy as sa

dctMonthTranslate={'Январь':'Jan', 'Февраль':'Feb', 'Март':'Mar', 'Апрель':'Apr', 'Май':'May', 'Июнь':'Jun', 'Июль':'Jul', 
 'Август':'Aug', 'Сентябрь':'Sep', 'Октябрь':'Oct', 'Ноябрь':'Nov', 'Декабрь':'Dec'}


def get_date_list(iYear):
    dates=['{0}-01-01'.format(str(iYear)), '{0}-01-01'.format(str(iYear+1))]
    start, end=[datetime.strptime(_, '%Y-%m-%d') for _ in dates]
    od=OrderedDict(((start+timedelta(_)).strftime(r'%b-%Y'), None) for _ in range((end-start).days))
    return list(od.keys())

def iterate_group(iterator, count):
    itr=iter(iterator)
    for i in range(0, len(iterator), count):
        yield iterator[i:i+count]

def print_last_point(name, date, val):
    print('{name}: last date = {date}, last val = {val}'.format(name=name, date=date, val=val))

class RTSDataFrame(pd.DataFrame):

    @property
    def _constructor(self):
        return RTSDataFrame

    def sort_index_date(self, inplace=False, ascending=True):
        self['tru_date']=self.index.map(lambda x:datetime.strptime(x, '%b-%Y'))
        dtf=self.sort_values(by='tru_date', inplace=False, ascending=ascending).drop('tru_date', axis=1)
        return dtf

    def index_to_datetime(self, format=r'%d-%m-%Y'):
        self.index=pd.to_datetime(self.index, format=format)
        return self

    def index_to_string(self, format=r'%d-%m-%Y'):
        self.index=self.index.map(lambda x: datetime.strftime(x, format))
        return self

    def to_sql(self, name, con, flavor='sqlite', schema=None, if_exists='fail', index=True,
               index_label=None, chunksize=10, dtype=None):

        def drop_table(strTName):
            meta=sa.MetaData(bind=con)
            try:
                tbl_=sa.Table(strTName, meta, autoload=True, autoload_with=con)
                tbl_.drop(con, checkfirst=False)
            except:
                pass
        
        def create_table(strTName, strIndName):
            metadata=sa.MetaData(bind=con)
            bname_t=sa.Table(strTName, metadata,
                        sa.Column(strIndName, sa.String, primary_key=True, nullable=False, autoincrement=False),
                        *[sa.Column(c_name, sa.Float, nullable=True) for c_name in self.columns])
            metadata.create_all()

        def buff_insert(alch_table, insert_prefix, values, buff_size=chunksize):
            for i in iterate_group(values, buff_size):
                inserter = alch_table.insert(prefixes=insert_prefix, values=i)
                con.execute(inserter)
             
        if if_exists=='replace':
            drop_table(name)
            if_exists='fail'
        
        if not con.dialect.has_table(con, name):
            create_table(name, self.index.name)
            
        meta=sa.MetaData(bind=con)
        tbl_names=sa.Table(name, meta, autoload=True, autoload_with=con)
        vals=self.reset_index().to_dict(orient='records')

        inserter=None

        if flavor == 'mysql':
            if if_exists in ['append', 'ignore']:
                inserter = tbl_names.insert(prefixes=['IGNORE'], values=vals)
            elif if_exists in ['update', 'upsert']:
                ins_state = sa.dialects.mysql.insert(tbl_names).values(vals)
                inserter = ins_state.on_duplicate_key_update(Date=ins_state.inserted.Date)
            elif if_exists=='fail':
                inserter = tbl_names.insert(values=vals)
            con.execute(inserter)
                
        if flavor == 'sqlite':
            if if_exists in ['append', 'ignore']:
                #inserter = tbl_names.insert(prefixes=['OR IGNORE'], values=vals)
                buff_insert(tbl_names, ['OR IGNORE'], vals, buff_size=chunksize)
            elif if_exists in ['update', 'upsert']:
                buff_insert(tbl_names, ['OR REPLACE'], vals, buff_size=chunksize)
                #inserter = tbl_names.insert(prefixes=['OR REPLACE'], values=vals)
            elif if_exists=='fail':
                buff_insert(tbl_names, None, vals, buff_size=chunksize)
        
