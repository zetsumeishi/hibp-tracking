import os
import re
from time import sleep

from peewee import (
    BigIntegerField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    ManyToManyField,
    Model,
    SqliteDatabase,
    TextField,
)
from hibp import hibp


DB = SqliteDatabase("db.sqlite")
with open('./emails.txt', 'r') as emails_file:
    EMAILS = emails_file.read().split('\n')


class DataClass(Model):
    name = CharField()

    class Meta:
        database = DB
        db_table = "data_classes"


class Breach(Model):
    name = CharField(unique=True)
    title = CharField(unique=True)
    domain = CharField()
    breach_date = DateField()
    added_date = DateField()
    modified_date = DateField()
    pwn_count = BigIntegerField()
    description = TextField()
    data_classes = CharField()
    is_verified = BooleanField()
    is_fabricated = BooleanField()
    is_sensitive = BooleanField()
    is_retired = BooleanField()
    is_spam_list = BooleanField()
    logo_path = CharField()

    class Meta:
        database = DB
        db_table = "breaches"


class Paste(Model):
    source = CharField()
    paste_id = CharField()
    title = CharField()
    date = DateTimeField()
    email_count = BigIntegerField()

    class Meta:
        database = DB
        db_table = "pastes"


class Account(Model):
    email = CharField()
    breaches = ManyToManyField(Breach, backref="accounts")
    pates = ManyToManyField(Paste, backref="accounts")

    class Meta:
        database = DB
        db_table = "accounts"


def camelcase_to_snakecase(input):
    """Transform a CamelCase string to snake_case"""
    output = re.sub(r"(?<!^)(?=[A-Z])", "_", input).lower()
    return output


def fill_database():
    """Initialize the database on the first run"""
    data_source = []

    print('Filling the database...')
    # Retrieve all emails in emails.txt at the root of the current folder
    for email in EMAILS:
        data_source.append({"email": email})

    # Bulk insert of Account objects
    with DB.atomic():
        Account.insert_many(data_source).execute()

    # Retrieve all the known breaches
    response = hibp.breaches()

    # Transform all keys to snake case for easy inserts
    if response.status_code == 200:
        data_source = []
        response = response.json()
        for known_breach in response:
            breach = {camelcase_to_snakecase(k): v for k, v in known_breach.items()}
            data_source.append(breach)

    # Bulk insert of Breach objects
    with DB.atomic():
        Breach.insert_many(data_source).execute()

    # Retrieve all the data classes names
    response = hibp.data_classes()

    # Prepare bulk insert
    if response.status_code == 200:
        data_source = []
        response = response.json()
        for data_class in response:
            data_source.append({'name': data_class})

    # Bulk insert of DataClass objects
    with DB.atomic():
        DataClass.insert_many(data_source).execute()


def update_accounts_breaches():
    """Iterate through all accounts and add the new breaches to the database"""
    accounts = Account.select()
    for account in accounts:
        # Use of sleep to avoid spamming HIBP servers
        sleep(1.5)
        data_source = []
        response = hibp.breached_account(account.email)

        if response and response.status_code == 200:
            response = response.json()
            for breach in response:
                breach_obj = Breach.get(Breach.name == breach['Name'])
                if breach_obj not in account.breaches:
                    data_source.append(breach_obj)
        account.breaches.add(data_source)


def main():
    if not os.path.exists("./db.sqlite"):
        # Initialize the junction table
        AccountBreaches = Account.breaches.get_through_model()
        DB.create_tables([DataClass, Breach, Account, AccountBreaches])
        fill_database()
    update_accounts_breaches()


if __name__ == "__main__":
    main()
