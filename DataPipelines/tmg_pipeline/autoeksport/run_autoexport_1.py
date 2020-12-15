if __name__ == '__main__':
    
    DROP_COLS = ['projectJob', 'id', 'rejectReason', 'endReason', 
                 #  '_numberOfSmsSent', 'smsOrderState', 'smsWorkflowOutcome', 'smsWorkflowState',
                 'sessionId', 'state', 'sessionUpdated', 'saleClosed', 'saleConfirmed',
                 'saleConfirmedMethod', 'saleCancelled', 'saleCancelledBy', 'lastCallState', 'contactResponse']
    
    PROJECTJOBS = ['project/job',]
    
    
    # Response file
    PROJECTJOBS = ['X/Y']
    SERVER = TmgServer(*settings.tmg_servers['inbound'])
    export = Autoeksport(projectJobs=PROJECTJOBS, 
                         exportfields='basic',
                         add_prefix=False,
                         session_state_in=None,
                         dialplan_contactresponse_in=None,
                         drop_cols=None,
                         horizontal_orders=False)

    # Get data
    prod = export._get_product_data()
    jc = export._get_jobcustomer_data()
    s = export._get_session_data()
        
    # Merge
    if export.horizontal_orders is True:
        result = jc.merge(s, how='left', on='id')
        result = result.merge(prod, how='left', on=['id', 'sessionId'])
    else:
        result = jc.merge(s, how='left', on='id')
        result = result.merge(prod, how='left', on=['id', 'sessionId'])
    
    # DROP
    if export.drop_cols is not None:
        result.drop(columns=export.drop_cols, inplace=True)
        log.debug('Dropped cols from self.drop_cols')        
    
    # Strip and rm \n
    result = result.replace({r'\s+$': '', r'^\s+': ''}, regex=True).replace(r'\n', ' ', regex=True)
    
    # Save
    export._save(result)
