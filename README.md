# ch-ch-ch-ch-changes

One way to look at the changes that happen day to day in the Chicago.

### So, what's going on here?

A long time ago, I worked on a now defunct project called
[Plenar.io](https://plenar.io). As part of the process of updating the various
datasets that we were downloading every day, we ended up with a daily snapshot
of everything that we loaded on a daily basis. I started getting curious about
how they might change in expected and unexpected ways so I started building
this as a little side project mostly to amuse myself. At some point, the folks
who were in charge of the daily ETLs that powered Plenar.io either deleted or
made private the S3 bucket where all the data was stored so I more or less put
it down at that point.
In the summer of 2024, I started remembering this project and how fun it was to
put together so I dusted it off and made some updates so that it's not relying
on data stored in the S3 bucket that was backing the Plenar.io project and
instead just downloads the Chicago Crime Report file directly from the Chicago
Data Portal. It's a real bummer to not have access to the years worth of files
that we downloaded daily for Plenar.io but it's still interesting and, with
time, might uncover some interesting things.

### Why Chicago Crime Reports?

Technically, the techniques used under the hood here could be adapted to work
with any dataset that gets updated with any frequency. The Chicago Crime
Reports data (or probably _any_ crime reports data) is interesting for a
few reasons. First, from a technical perspective it can be challenging to work
with since it's well over 8 million rows and growing every day. And secondly,
and probably more interestingly, maybe someone should be paying attention to
how this file changes. A lot of the time, there's a reasonable explanation for
why a certain crime report has a particular change. For example, the arrest
field goes from "false" to "true", which seems like progress. Other times,
perhaps there's something else going on that's maybe hiding a larger trend. One
thing that I've built into this project is a view which tracks when reports go
from an index crime (meaning they need to be reported to the FBI) to a
non-index crime and vice versa. I make no claims as to what this means or that
there is something fishy going on here. I just wanted to explore what's
possible and perhaps get others thinking about it as well.

### Getting setup

This is a pretty standard Python & Flask project backed by a PostgreSQL
database. To get started you'll want to create a virtual environment and
install the requirements:

```
cd /path/to/cloned/repo
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Obviously, you can use whatever virtual environment wrapper (virtualenvwrapper,
pyenv, poetry, etc) that you prefer, too. 

By default, the application expects a PostgreSQL database called `changes` to
be running on `localhost` on port `5432` with the username `changes` and the
password `changes-password`. If you want to modify that, make sure you export
the appropriate environment variables to override those settings before running
the app:

```
DB_USER
DB_PASS
DB_HOST
DB_PORT
DB_NAME
```

### Running

To run the app, `cd` into the `changes` directory and run `flask run`

```
cd changes
flask run
```

The app should now be running on `http://127.0.0.1:5000`. 

### Loading data

You won't be able to see anything in the various views until you load a couple
days worth of data.  To do that, make sure you're in the `changes` directory
still and then run `flask run-etl`.  This will start downloading the necessary
files from the Chicago Data Portal into the `changes` directory and perform all
the necessary transformations.  This usually takes around 15-20 minutes (about
10 of those minutes are just downloading the crime reports file from the data
portal. They really seem to like to throttle big downloads so just be patient).

A few options that you have with the ETL runner are to change the directory
where the files are downloaded (which you might want to do since the main crime
report file is 1.8GB currently and is only getting bigger) and which date to
load. The former option is pretty straightforward, just supply a path with the
`--storage-dir` flag:

```
flask run-etl --storage-dir /path/to/another/place
```

The other option, changing which day's file you're loading, really only works
if you have that file downloaded already. So, if for instance you've been
    loading data for a week or two and have all those daily files stored
    someplace, you can recreate the DB from scratch by just running the ETL
    with the path to the place where the files are stored and telling it which
    day's file to load like so:

```
flask run-etl --storage-dir /path/to/storage --file-date 2024-01-01
flask run-etl --storage-dir /path/to/storage --file-date 2024-01-02
flask run-etl --storage-dir /path/to/storage --file-date 2024-01-03
flask run-etl --storage-dir /path/to/storage --file-date 2024-01-04

... etc ...
```

It'll take probably about 5 or so minutes to complete each day since it won't
need to re-download those files.

### Running in docker

I've included a rudimentary Dockerfile and docker-compose file which should at
least do the basics of running a DB and the app. You'll still need to run the
ETL separately. To get that up and running, just do:

```
cd /path/to/cloned/repo
docker compose up
```

### FAQs

**Hey, you're using a _really old_ version of Bootstrap. Why don't you upgrade?**

Why don't you look at the wicked ass SQL in the guts of this project? Until
something actually breaks in the UI, I'm probably not going to do anything
about that.

**Man, you've got some wicked ass SQL in the guts of this project. Why don't you use an ORM?**

Sometimes when you've spent half a day trying to figure out the correct way to
ask a database a question, you don't feel like translating that question into
another language (that often times just makes the code less readable anyways).

**This thing is totally not production ready. Why don't you use a WSGI server and tune PostgreSQL a bit for those big writes that are happening in the ETL?**

It looks like you just found yourself a really great open source contribution.
Also, I'm not really interested in hosting a public facing version of this at
the time so I'm not too worried about productionalizing it.
