# ch-ch-ch-ch-changes

[![CI](https://github.com/evz/ch-ch-ch-changes/actions/workflows/ci.yml/badge.svg)](https://github.com/evz/ch-ch-ch-changes/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A tool for tracking changes in Chicago's crime dataset over time.

## Tech Stack
- Python 3, Flask, SQLAlchemy
- PostgreSQL 
- Docker
- Chicago Data Portal API

## What it does
- Downloads daily Chicago crime data (8+ million records, ~1.8GB)
- Detects changes between daily snapshots
- Tracks when crime classifications change (e.g., index to non-index crimes)
- Provides a web interface to browse changes

### Why Chicago Crime Reports?

The [Chicago Crime Reports
data](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data)
is interesting for a few reasons. From a technical perspective it's challenging
to work with - over 8 million rows and growing daily. But more importantly,
maybe someone should be paying attention to how this file changes. 

A lot of the time, there's a reasonable explanation for changes. The arrest
field goes from "false" to "true" - that seems like progress. Other times,
perhaps there's something else going on that's hiding a larger trend. I've
built in a view that tracks when reports go from an index crime (reported to
the FBI) to a non-index crime and vice versa. I make no claims about what this
means or that there's something fishy going on. I just wanted to explore what's
possible and get others thinking about it too.

**Why this matters**

Crime data gets updated frequently, but the Chicago Data Portal doesn't
maintain historical snapshots - there's no official record of how reports
change over time. This project creates an independent archive to track those
changes and explore what they might reveal.

### So, what's going on here?

A long time ago, I worked on a now defunct project called
[Plenar.io](https://plenar.io). As part of the process of updating the various
datasets that we were downloading every day, we ended up with a daily snapshot
of everything that we loaded. I started getting curious about how things might
change in expected and unexpected ways so I started building this as a little
side project mostly to amuse myself. At some point, the folks took over the
daily ETLs that powered Plenar.io either deleted or made private the S3 bucket
where all the data was stored so I more or less put it down at that point.

In the summer of 2024, I started remembering this project and how fun it was to
put together so I dusted it off and made some updates so that it's not relying
on data stored in the S3 bucket that was backing the Plenar.io project and
instead just downloads the Chicago Crime Report file directly from the Chicago
Data Portal. It's a real bummer to not have access to the years worth of files
that we downloaded daily for Plenar.io but it's still interesting and, with
time, might uncover some interesting things.

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

### Development with Makefile

A Makefile wraps common Docker operations:

```bash
make help        # Show available commands
make build       # Build Docker images
make up          # Start application and database
make etl         # Run ETL process (downloads ~1.8GB, takes 15-20 minutes)
make test        # Run test suite
make lint        # Run code linting
make format      # Format code with black and isort
make check       # Check code formatting without changes
make logs        # Show application logs
make shell       # Open shell in application container
make db-shell    # Open PostgreSQL shell
make down        # Stop services
make clean       # Stop containers and remove volumes
```

**Quick start:**
```bash
make build
make up
make etl
# Open http://localhost:5000
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
