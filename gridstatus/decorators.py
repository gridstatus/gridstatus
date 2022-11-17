import functools
import os
import pprint

import pandas as pd
import tqdm

import gridstatus
from gridstatus.base import Markets


def _get_args_dict(fn, args, kwargs):
    args_names = fn.__code__.co_varnames[: fn.__code__.co_argcount]
    return {**dict(zip(args_names, args)), **kwargs}


class support_date_range:
    def __init__(self, frequency, update_dates=None):
        """Maximum frequency of ranges"""
        self.frequency = frequency
        self.update_dates = update_dates

    def __call__(self, f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            args_dict = _get_args_dict(f, args, kwargs)

            save_to = None
            if "save_to" in args_dict:
                save_to = args_dict.pop("save_to")
                os.makedirs(save_to, exist_ok=True)

            error = "ignore"
            errors = []
            if "error" in args_dict:
                error = args_dict.pop("error")

            if "date" in args_dict and "start" in args_dict:
                raise ValueError(
                    "Cannot supply both 'date' and 'start' to function {}".format(
                        f,
                    ),
                )

            if "date" not in args_dict and "start" not in args_dict:
                raise ValueError(
                    "Must supply either 'date' or 'start' to function {}".format(
                        f,
                    ),
                )

            if "start" in args_dict:
                args_dict["date"] = args_dict["start"]
                del args_dict["start"]

            if args_dict["date"] == "latest":
                return f(*args, **kwargs)

            if (
                isinstance(args_dict["date"], str)
                and args_dict["date"].lower() == "today"
            ):
                args_dict["date"] = pd.Timestamp.now(
                    tz=args_dict["self"].default_timezone,
                ).date()

            args_dict["date"] = gridstatus.utils._handle_date(
                args_dict["date"],
                args_dict["self"].default_timezone,
            )

            # no date range handling required
            if "end" not in args_dict:
                df = f(**args_dict)
                _handle_save_to(df, save_to, args_dict, f)
                return df
            else:
                if (
                    isinstance(args_dict["end"], str)
                    and args_dict["end"].lower() == "today"
                ):
                    # add one day since end is exclusive
                    args_dict["end"] = pd.Timestamp.now(
                        tz=args_dict["self"].default_timezone,
                    ).date() + pd.DateOffset(days=1)

                args_dict["end"] = gridstatus.utils._handle_date(
                    args_dict["end"],
                    args_dict["self"].default_timezone,
                )

                assert (
                    args_dict["end"] > args_dict["date"]
                ), "End date {} must be after start date {}".format(
                    args_dict["end"],
                    args_dict["date"],
                )

            # use .date() to remove timezone info, which doesnt matter if just a date

            # Note: this may create a split that will end up being unnecessary after running update dates below.
            # that is because after adding new dates, it's possible that two ranges could be added.
            # Unnecessary optimization right now to include logic to handle this
            try:
                dates = pd.date_range(
                    args_dict["date"].date(),
                    args_dict["end"].date(),
                    freq=self.frequency,
                    inclusive="neither",
                )
                dates = [args_dict["date"]] + dates.tolist() + [args_dict["end"]]
            except TypeError:
                dates = pd.date_range(
                    args_dict["date"].date(),
                    args_dict["end"].date(),
                    freq=self.frequency,
                    closed="left",
                )
                # no option for closed neither :(
                dates = dates.tolist()
                if len(dates) == 0 or args_dict["date"].date() != dates[0].date():
                    dates = [args_dict["date"]] + dates
                dates = dates + [args_dict["end"]]

            # add end date since it's not included

            dates = [
                gridstatus.utils._handle_date(
                    d,
                    args_dict["self"].default_timezone,
                )
                for d in dates
            ]

            # sometime api have restrictions/optimizations based on date ranges
            # update_dates allows for the caller to insert this logic
            if self.update_dates is not None:
                dates = self.update_dates(dates, args_dict)

            start_date = dates[0]

            # remove end date and add back later if needed
            del args_dict["end"]

            all_df = []

            # every None removes two possible queries
            total = len(dates) - dates.count(None) * 2 - 1

            with tqdm.tqdm(disable=total <= 1, total=total) as pbar:

                for end_date in dates[1:]:
                    # if we come across None, it means we should reset
                    if end_date is None:
                        start_date = None
                        continue

                    # if start_date is None, we just reset and end is actually the start
                    if start_date is None:
                        start_date = end_date
                        continue

                    args_dict["date"] = start_date

                    # no need for end if we are querying for just 1 day
                    if self.frequency != "1D":
                        args_dict["end"] = end_date

                    try:
                        df = f(**args_dict)
                    except Exception as e:
                        if error == "raise":
                            raise e
                        elif error == "ignore":
                            df = None
                            errors += [args_dict.copy()]
                            print("Error: {}".format(e))
                            print("Args: {}\n".format(args_dict))
                        else:
                            raise ValueError(
                                "Invalid value for error: {}".format(
                                    error,
                                ),
                            )

                    _handle_save_to(df, save_to, args_dict, f)

                    pbar.update(1)

                    if df is not None:
                        all_df.append(df)

                    start_date = end_date

            if errors:
                print("Errors that occurred while getting data:")
                pprint.pprint(errors)

            df = pd.concat(all_df).reset_index(drop=True)

            return df

        return wrapped_f


def _handle_save_to(df, save_to, args_dict, f):
    if df is not None and save_to is not None:
        if "end" in args_dict:
            filename = "{}_{}_{}_{}.csv".format(
                args_dict["self"].__class__.__name__,
                f.__name__,
                args_dict["date"].strftime("%Y%m%d"),
                args_dict["end"].strftime("%Y%m%d"),
            )
        else:
            filename = "{}_{}_{}.csv".format(
                args_dict["self"].__class__.__name__,
                f.__name__,
                args_dict["date"].strftime("%Y%m%d"),
            )

        path = os.path.join(save_to, filename)

        df.to_csv(path, index=None)


def _get_pjm_archive_date(market):
    import gridstatus

    market = Markets(market)
    tz = gridstatus.PJM.default_timezone
    if market == Markets.REAL_TIME_5_MIN:
        archive_date = pd.Timestamp.now(
            tz=tz,
        ) - pd.Timedelta(days=186)
    elif market == Markets.REAL_TIME_HOURLY:
        archive_date = pd.Timestamp.now(
            tz=tz,
        ) - pd.Timedelta(days=731)
        # todo implemlement location type filter
    elif market == Markets.DAY_AHEAD_HOURLY:
        archive_date = pd.Timestamp.now(
            tz=tz,
        ) - pd.Timedelta(days=731)

    return archive_date.replace(hour=0, minute=0, second=0, microsecond=0)


def pjm_update_dates(dates, args_dict):
    """PJM has a weird API. This method updates the date range list to account
    for the following restrictions:

     - date ranges cannot span year boundaries
     - date ranges cannot span archive / standard boundaries
     - date range is inclusive of start and end dates
    """

    archive_date = _get_pjm_archive_date(args_dict["market"])

    new_dates = []

    for i, date in enumerate(dates):
        # stop if last date
        if i + 1 == len(dates):
            # add last date if new range has started
            if new_dates[-1] is not None:
                new_dates.append(date)

            break

        new_dates.append(date)

        # restriction 1: year boundary
        next_date = dates[i + 1]
        for year in range(date.year, next_date.year):
            current_year_end = pd.Timestamp(
                year=year,
                month=12,
                day=31,
                hour=23,
                minute=59,
                tz=args_dict["self"].default_timezone,
            )
            new_dates.append(current_year_end)
            next_year_start = pd.Timestamp(
                year=year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                tz=args_dict["self"].default_timezone,
            )

            new_dates.append(None)  # signal to skip to next date

            # dont need another range if the range ends at the start of the next year
            if next_year_start != next_date:
                new_dates.append(next_year_start)

    # remove trailing None
    if new_dates[-1] is None:
        new_dates = new_dates[:-1]

    # restriction 2: archive / standard boundary
    for i, date in enumerate(new_dates[:-1]):
        next_date = new_dates[i + 1]
        # check if archive date is between date and next_date
        if None not in [date, next_date] and date < archive_date < next_date:
            day_before_archive = archive_date - pd.Timedelta(days=1)
            add_before = pd.Timestamp(
                year=day_before_archive.year,
                month=day_before_archive.month,
                day=day_before_archive.day,
                hour=23,
                minute=59,
                tz=args_dict["self"].default_timezone,
            )

            new_dates = (
                new_dates[: i + 1]
                + [
                    add_before,
                    None,
                    archive_date,
                ]
                + new_dates[i + 1 :]
            )

    return new_dates
