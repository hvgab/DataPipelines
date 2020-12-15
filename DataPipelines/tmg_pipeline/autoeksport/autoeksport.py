
import pandas as pd
import os
import logging
import datetime as dt
from timmytimer import timmy
import settings

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
log = logging.getLogger(__name__)

class TmgServer():
    def __init__(self, name, host, dbname, username, password):
        self.name=name
        self.host=host
        self.dbname=dbname
        self.username=username
        self.password=password
    
    def __str__(self) -> str:
        return f'{self.host} {self.dbname} {self.username} {self.password}'

class Autoeksport(object):

    BASIC_DROP_COLS = {
        "JOBCUSTOMER": ['version', 'dirtyStatisticsDays', 'customerType', 'prospectReason', 'prospectBy', 'prospect', 'leadCreatedBy'],
        "SESSION": ["projectJob", "customerId", "agent", "agentId", "sessionUpdatedReference", "saleClosedReference",
                    "backofficeVerified", "backofficeVerifiedBy", "backofficeRejected", "backofficeRejectedBy", "backofficeNote",
                    "externalVerified", "externalVerifiedBy", "externalOrderId",
                    "saleClosedBy", "saleConfirmedBy"],
        "DIALPLAN": ["projectJob", "customerId", "project", "job", "lastDialHistRef",
                     "phoneNoCountryCode", "phoneNo2CountryCode", "phoneNo3CountryCode",
                     "loadDate", "uploadId", "pdCallPriority", "pdCallPriorityValidThrough",
                     "dialplanReferenceDate", "dialplanReferenceTime", "processingAddressBy",
                     "noContactDescription", "dialTimeToAnsweringMachinePhoneNo",
                     "dialTimeToAnsweringMachinePhoneNo2", "dialTimeToAnsweringMachinePhoneNo3",
                     "cntRedial", "cntNoAnswer", "cntBusy", "cntWrongPerson", "cntNoAnswerWorkshift",
                     "cntAbandonToday", "cntAbandon", "cntCallBack", "cntAnswer",
                     "cntAnsweringMachine", "cntExtensionFailurePhoneNo", "cntExtensionFailurePhoneNo2",
                     "cntExtensionFailurePhoneNo3", "cntMessageAttempt",
                     "cntMessageAttemptWithFailure", "block", "disqualified", "domain", "cntTotRedial",
                     "cntTotNoAnswer", "cntTotWrongPerson", "cntTotAbandon"
                     ],
        "PRODUCT": ['cpo','agentProvisionPercent','agentProvisionPoints']
        # "PRODUCT": ['projectJob', 'id', 'customerId', 'sessionId', 'productId', 'productName', 'freight', 'unitPriceToCustomer', 'fixedPrice', 'discountInPercent','vat','cpo','agentProvisionPercent','agentProvisionPoints','quantum','salesAmount','deriveSalesAmount']
    }

    def __init__(self, 
                 tmg_server,
                 project=None,
                 projectJobs=None, 
                 add_prefix=False, 
                 exportfields='basic', 
                 session_state_in=None, 
                 dialplan_contactresponse_in=None, 
                 drop_cols=None,
                 horizontal_orders=False):
        """[summary]

        Args:
            project ([type], optional): [description]. Defaults to None.
            projectJobs ([type], optional): [description]. Defaults to None.
            add_prefix (bool, optional): [description]. Defaults to True.
            fields (str, optional): [description]. Defaults to 'basic'. Either 'full' or 'basic'
            dialplan_contactresponse_in (list of str, optional): If 'NULL' checks if null instead.

        Raises:
            ValueError: [description]
        """
        self.tmg_server = tmg_server
        
        if project is None and projectJobs is None:
            raise ValueError('You have to specify a project.')
        self.project = project
        self.projectLike = f'{self.project}/%'
        self.projectJobs = projectJobs
        if self.projectJobs is not None:
            self.projectJobsQuery = ', '.join(f"'{projectJob}'" for projectJob in self.projectJobs)
            self.projectJobsQuery = f'({self.projectJobsQuery})'
        
        self.session_state_in = session_state_in
        if self.session_state_in is not None:
            self.session_state_in_query = ', '.join(f"'{_}'" for _ in self.session_state_in)
        
        self.dialplan_contactresponse_in = dialplan_contactresponse_in
        if self.dialplan_contactresponse_in is not None:
            self.dialplan_contactresponse_in_query = ', '.join(f"'{_}'" for _ in self.dialplan_contactresponse_in)
        
        self.exportfields = exportfields
        self.add_prefix = add_prefix
        self.drop_cols = drop_cols
        self.horizontal_orders = horizontal_orders
        
        self.DB_USERNAME = os.getenv('DB_USERNAME')
        self.DB_PASSWORD = os.getenv('DB_PASSWORD')
        self.DB_HOST = os.getenv('DB_HOST')
        self.conn = f'mysql+pymysql://{self.tmg_server.username}:{self.tmg_server.password}@{self.tmg_server.host}/tmg_customer'
        log.debug(f'self.conn: {self.conn}')
        
    def __str__(self) -> str:
        return f'<AutoExport({self.project}, {self.projectJobs})>'
    
    def __repr__(self) -> str:
        return f'<AutoExport({self.project}, {self.projectJobs})>'

    @timmy
    def main_export(self):
        """Export for customer

        Args:
            type (str, optional): [description]. Defaults to 'basic'. (basic, full)
        """        
        # Get Data
        prod = self._get_product_data()
        jc = self._get_jobcustomer_data()
        s = self._get_session_data()
        cf = self._get_customerfield_data()
        sf = self._get_sessionfield_data()
        dp = self._get_dialplan_data()
        
        # Merge
        # These are the same, just different column names to join on.
        if self.add_prefix:
            result = jc.merge(cf, how='left', left_on='jc_id', right_on='cf_id')
            if self.session_state_in is not None:
                result = result.merge(s, how='right', left_on='jc_id', right_on='s_id')
            else:
                result = result.merge(s, how='left', left_on='jc_id', right_on='s_id')
            result = result.merge(sf, how='left', left_on=['s_id', 's_sessionId'], right_on=['sf_id', 'sf_sessionId'])
            result = result.merge(prod, how='left', left_on=['s_id', 's_sessionId'], right_on=['prod_id', 'prod_sessionId'])
            result = result.merge(dp, how='left', left_on='jc_id', right_on='dp_id')
        else:
            result = jc.merge(cf, how='left', on='id')
            if self.session_state_in is not None:
                result = result.merge(s, how='right', on='id')
            else:
                result = result.merge(s, how='left', on='id')
            result = result.merge(sf, how='left', on=['id', 'sessionId'])
            result = result.merge(prod, how='left', on=['id', 'sessionId'])
            result = result.merge(dp, how='left', on='id')
        
        # Merge _x and _y
        # TODO: Merged cols will place last. Replace to x or y pos as default? Then let customer reorder in separate field?
        cols = []
        for col in result.columns:
            if col.endswith('_x'):
                cols.append(col.replace('_x', ''))
        log.debug(cols)
        
        for col in cols:
            xy_cols = [f'{col}_x', f'{col}_y']
            result[col] = result[xy_cols].apply(lambda x: ', '.join(x.dropna().astype(str)), 1)
            result = result.drop(xy_cols, axis=1)
            log.debug(f'Merged and dropped _x, _y for {col}')
            
        log.info('Merged and dropped _x, _y')
        
        # DROP
        if self.drop_cols is not None:
            result.drop(columns=self.drop_cols, inplace=True)
            log.debug('Dropped cols from self.drop_cols')        
        
        # Strip and rm \n
        result = result.replace({r'\s+$': '', r'^\s+': ''}, regex=True).replace(r'\n', ' ', regex=True)
        
        # Save
        self._save(result)

    @timmy
    def _get_jobcustomer_data(self):
        log.debug('Getting jobcustomer data')
        
        # PRE SQL
        query = """SELECT * FROM tmg_customer.jobCustomer """
        if self.project is not None:
            query += f" WHERE projectJob like '{self.projectLike}' "
        elif self.projectJobs is not None:
            query += f" WHERE projectJob in {self.projectJobsQuery} "  # TODO: Prone to SQL Injection?
            log.debug(f'query: {query}')
        
        df = pd.read_sql(query, self.conn)
        # df = pd.read_sql(query, self.conn, params={"name":self.projectJob})
        
        # POST SQL
        df.rename(columns={"note": "customerNote"}, inplace=True)
        
        if self.exportfields == 'basic':
            df.drop(columns=self.BASIC_DROP_COLS["JOBCUSTOMER"], inplace=True)
            log.debug('Dropped cols down to basic')
        
        if self.add_prefix:
            df = df.add_prefix('jc_')
        return df

    @timmy
    def _get_customerfield_data(self):
        log.debug('Getting customerfield data')
        query = """SELECT cf.* 
                FROM tmg_customer.customerField cf """
                # WHERE cf.projectJob like %(name)s"""
        if self.project is not None:
            query += " WHERE projectJob like %(name)s "
            df = pd.read_sql(query, self.conn, params={"name": self.projectLike})
        elif self.projectJobs is not None:
            query += f" WHERE projectJob in {self.projectJobsQuery} "  # TODO: Prone to SQL Injection?
            df = pd.read_sql(query, self.conn)
        # df = pd.read_sql(query, self.conn, params={"name":self.projectJob})
        
        log.debug('Fixing customerfield df')
        df.dropna(axis=0, how='any', inplace=True)
        df = df.pivot(index='id', columns='element', values='value')
        df.reset_index(inplace=True)
        if self.add_prefix:
            df = df.add_prefix('cf_')
        return df

    @timmy
    def _get_session_data(self):
        log.debug('Getting session data')
        
        # PRE SQL
        query = """SELECT *
                FROM tmg_customer.session s """
        if self.project is not None:
            query += f" WHERE projectJob like '{self.projectLike}' "
            # df = pd.read_sql(query, self.conn, params={"name":self.projectLike})
        elif self.projectJobs is not None:
            query += f" WHERE projectJob in {self.projectJobsQuery} "  # TODO: Prone to SQL Injection?
        
        if self.session_state_in is not None:
            query += f' AND state in ({self.session_state_in_query}) '
            
        log.debug(f'session query: {query}')
        
        
        # SQL
        df = pd.read_sql(query, self.conn)
        
        # Post SQL
        df.rename(columns={"note": "sessionNote"}, inplace=True)
        
        if self.exportfields == 'basic':
            df.drop(columns=self.BASIC_DROP_COLS["SESSION"], inplace=True)
            log.debug('Dropped cols down to basic')
            
        if self.add_prefix:
            df = df.add_prefix('s_')
        return df
    
    @timmy
    def _get_sessionfield_data(self):
        log.debug('Getting sessionfield data')
        
        query = """SELECT sf.* FROM tmg_customer.sessionField sf """
            # WHERE sf.projectJob like %(name)s"""
        if self.project is not None:
            query += f" WHERE projectJob like '{self.projectLike}' "
            # df = pd.read_sql(query, self.conn, params={"name": self.projectLike})
        elif self.projectJobs is not None:
            query += f" WHERE projectJob in {self.projectJobsQuery} "  # TODO: Prone to SQL Injection?
        
        # session state
        # if self.session_state_in is not None:
        #     query += f' AND state in ({self.session_state_in_query})'
        
        log.debug(f'SessionField query: {query}')
        df = pd.read_sql(query, self.conn)
            
        
        log.debug('Fixing sessionfield df')
        try:
            df = df.pivot(index=['id', 'sessionId'], columns='element', values='value')
            df.reset_index(inplace=True)
        except Exception as e:
            log.error(f'SessionField Pivot Fail\t{e}')
            exit(0)
        if self.add_prefix:
            df = df.add_prefix('sf_')
        return df

    @timmy
    def _get_dialplan_data(self):
        log.debug('getting dialPlan data')
        
        query = """SELECT dp.* FROM tmg_customer.dialPlan dp """
                # WHERE dp.projectJob LIKE %(name)s"""
        if self.project is not None:
            query += f" WHERE projectJob like '{self.projectLike}' "
            # df = pd.read_sql(query, self.conn, params={"name":self.projectLike})
        elif self.projectJobs is not None:
            query += f" WHERE projectJob in {self.projectJobsQuery} "  # TODO: Prone to SQL Injection?
            
        if self.dialplan_contactresponse_in is not None:
            if self.dialplan_contactresponse_in == 'NULL' or self.dialplan_contactresponse_in == ['NULL']:
                query += ' AND contactResponse IS NULL '
            else:
                query += f' AND contactResponse in ({self.dialplan_contactresponse_in_query}) '
            
        df = pd.read_sql(query, self.conn)
        # df = pd.read_sql(query, self.conn, params={"name":self.projectJob})
        
        if self.exportfields == 'basic':
            df.drop(columns=self.BASIC_DROP_COLS["DIALPLAN"], inplace=True)
            log.debug('Dropped cols down to basic')
            
        if self.add_prefix:
            df = df.add_prefix('dp_')
        return df
    
    @timmy
    def _get_product_data(self):
        log.debug('getting product data')
        
        # Query builder
        query = """ SELECT p.*, pd.displayedTextToAgent, pd.writtenToFileValue 
                    FROM tmg_customer.product p 
                    JOIN tmg_config.productDef pd on p.productId = pd.productId and substring_index(p.projectJob, '/', 1) = pd.project """
        if self.project is not None:
            query += f" WHERE projectJob like '{self.projectLike}' "
        elif self.projectJobs is not None:
            query += f" WHERE projectJob in {self.projectJobsQuery} "  # TODO: Prone to SQL Injection?
            
        df = pd.read_sql(query, self.conn)
        
        print('\n\tPRODUCTS\n')
        print(df)
        # Pivot Orders?
        if self.horizontal_orders is True:
            log.debug('Pivot Orders to Horizontal')
            try:
                # df = df.pivot(index=['id'], columns=['sessionId'])
                columns = list(df.columns)
                columns.remove('id')
                columns.remove('sessionId')
                print(columns)
                df = df.pivot(index=['id'], columns=columns, values=['sessionId'])
                print(df)
                input()
                df.to_excel('products_pivot-2.xlsx')
                df.reset_index(inplace=True)
                print(df)
                df.to_excel('products_pivot_reset_index-2.xlsx')
            except Exception as e:
                log.error(f'Product Pivot Fail\t{e}')
                exit(0)
        else:
            pass  # TODO
        
        # Drop cols
        if self.exportfields == 'basic':
            df.drop(columns=self.BASIC_DROP_COLS["PRODUCT"], inplace=True)
            log.debug('Dropped cols down to basic')
        
        # Add prefix
        if self.add_prefix:
            df = df.add_prefix('prod_')
        
        return df

    @timmy
    def _save(self, df, filename=None):
        # Create filename
        if filename is None:
            if self.project != None:
                project = self.project
            elif self.projectJobsQuery != None:
                project = self.projectJobsQuery = '-'.join(self.projectJobs)
            else:
                project = '?project?'
            
            server = self.tmg_server.name.upper()
            filename = f'{server}-{project}'
                
        # filename fix
        filename = filename.replace('/', '-').replace('.', '-').replace('-%', '-AllJobs')
        
        # add timestamp
        timestamp = dt.datetime.now().strftime('%Y%m%dT%H%M%S')
        
        log.debug(f'Saving df to file ({filename})')
        filename = f'{filename}-{timestamp}.xlsx'
        df.to_excel(filename, index=False)
        log.info(f'Saved {filename}')
        
    