# Prom

An opinionated lightweight orm for Postgres

## Example -- Create a User class

Prom tries to make it as easy as possible on the developer to set common options, so you don't have to constantly look at the documentation until you need to change something significant.

Here is how you would define a new Orm class:

    # app.models (app/models.py)
    import prom

    class User(prom.Orm):

        schema = prom.Schema(
            "user_table_name", # the db table name
            username=(str, True), # string field (required)
            password=(str, True), # string field (required)
            email=(str, True), # string field (required)
            is_admin=(int,), # integer field (not required)
            unique_user=('username') # set a unique index on username field
            index_email=('email') # set a normal index on email field
        )

You can specify the connection using an environment and a prom dsn url:

    
    <full.python.path.InterfaceClass>://<username>:<password>@<host>:<port>/<database>?<options=val&query=string>#<name>

So to use the builtin Postgres interface on `testdb` database `localhost` with username `testuser` and password `testpw`:

    prom.PostgresInterface://testuser:testpw@localhost/testdb

To use our new User class:

    # testprom.py
    import prom
    from app.models import User

    prom.configure("prom.PostgresInterface://testuser:testpw@localhost/testdb")

    # create a user
    u = User(username='foo', password='awesome_and_secure_pw_hash', email='foo@bar.com')
    u.set()

    # query for our new user
    u = User.query.is_username('foo').get_one()
    print u.username # foo

    get the user again via the primary key:
    u2 = User.query.get_pk(u.pk)
    print u.username # foo

    let's add a bunch more users:
    for x in xrange(10):
        username = "foo{}".format(x)
        ut = User(username=username, password="...", email="{}@bar.com".format(username))
        ut.set()

    # now let's iterate through all our new users:
    for u in User.query.get():
        print u.username

## Opinionated

Prom is pretty opinionated in what it does, it assumes you don't want to do joins, and that you never want to do an `OR` query on two different fields.


