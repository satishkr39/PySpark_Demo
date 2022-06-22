import redshift_connector

# Connects to Redshift cluster using AWS credentials
conn = redshift_connector.connect(
    host='redshift-cluster-1.cbt57brglsv6.us-east-1.redshift.amazonaws.com',
    database='dev',
    user='awsuser',
    password='Satishpassword#123'
 )

cursor: redshift_connector.Cursor = conn.cursor()
cursor.execute("create Temp table employee(firstname varchar,lastname varchar)")
cursor.executemany("insert into employee (firstname, lastname) values (%s, %s)",
                    [
                        ('John', 'Smith'),
                        ('Mike', 'Tatum')
                    ]
                  )
cursor.execute("select * from category")

result: tuple = cursor.fetchall()
print(result)