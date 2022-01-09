# SUPPORT FOR
# ANALYZE TABLE dataset
# [{'Analyze': {'table_name': [{'value': 'be', 'quote_style': None}], 'partitions': None, 'for_columns': False, 'columns': [], 'cache_metadata': False, 'noscan': False, 'compute_statistics': False}}]

# FUTURE SUPPORT FOR
# ANALYZE TABLE dataset PARTITION(date = '2021-01-02');
# [{'Analyze': {'table_name': [{'value': 'be', 'quote_style': None}], 'partitions': [{'BinaryOp': {'left': {'Identifier': {'value': 'date', 'quote_style': None}}, 'op': 'Eq', 'right': {'Value': {'SingleQuotedString': '2021-01-02'}}}}], 'for_columns': False, 'columns': [], 'cache_metadata': False, 'noscan': False, 'compute_statistics': False}}]
