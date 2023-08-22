#compdef tccon-priors-cli

autoload -U is-at-least

_tccon-priors-cli() {
    typeset -A opt_args
    typeset -a _arguments_options
    local ret=1

    if is-at-least 5.2; then
        _arguments_options=(-s -S -C)
    else
        _arguments_options=(-s -C)
    fi

    local context curcontext="$curcontext" state line
    _arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli_commands" \
"*::: :->tccon-priors-cli" \
&& ret=0
    case $state in
    (tccon-priors-cli)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-command-$line[1]:"
        case $line[1] in
            (met)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__met_commands" \
"*::: :->met" \
&& ret=0

    case $state in
    (met)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-met-command-$line[1]:"
        case $line[1] in
            (check)
_arguments "${_arguments_options[@]}" \
'-m+[The key identifying the section in the configuration file to use for the set of met files required. In a configuration file with sections "\[\[data.download.geosfpit\]\]", the key would be "geosfpit". If not given, the default met(s) are checked]:MET_KEY: ' \
'--met=[The key identifying the section in the configuration file to use for the set of met files required. In a configuration file with sections "\[\[data.download.geosfpit\]\]", the key would be "geosfpit". If not given, the default met(s) are checked]:MET_KEY: ' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':start-date -- The first date to check, in YYYY-MM-DD format:' \
'::end-date -- The day AFTER the last date to check, if omitted, only START_DATE is checked:' \
&& ret=0
;;
(download-dates)
_arguments "${_arguments_options[@]}" \
'-d[Print what would be downloaded but do not download anything]' \
'--dry-run[Print what would be downloaded but do not download anything]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':met-key -- The key used in your TOML configuration file to declare a meteorology type. If you have \[\[data.download.geosit\]\] for example, then the key would be "geosit":' \
':start-date -- The first date to download data for, in yyyy-mm-dd format:' \
'::end-date -- The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given the default is one day after start_date (i.e. just download for start_date):' \
&& ret=0
;;
(download-missing)
_arguments "${_arguments_options[@]}" \
'-s+[The first date to download data for, in yyyy-mm-dd format. If not given, it will default to the most recent day that has all the expected met data for the given met_key. If no complete days are present, it will use the earliest "earliest_date" value in the TOML download sections for this met_key]:START_DATE: ' \
'--start-date=[The first date to download data for, in yyyy-mm-dd format. If not given, it will default to the most recent day that has all the expected met data for the given met_key. If no complete days are present, it will use the earliest "earliest_date" value in the TOML download sections for this met_key]:START_DATE: ' \
'-e+[The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given, it defaults to today (and so will try to download met data through yesterday)]:END_DATE: ' \
'--end-date=[The last date (exclusive) to download data for, in yyyy-mm-dd format. If not given, it defaults to today (and so will try to download met data through yesterday)]:END_DATE: ' \
'-m+[]:MET_KEY: ' \
'--met=[]:MET_KEY: ' \
'-i[]' \
'--ignore-defaults[]' \
'-d[Set this flag to print what would be downloaded, but not actually download anything]' \
'--dry-run[Set this flag to print what would be downloaded, but not actually download anything]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(rescan)
_arguments "${_arguments_options[@]}" \
'-s+[The first date to check for data, in yyyy-mm-dd format. If not given, it will default to a sensible value, depending on the value of --met-key]:START_DATE: ' \
'--start-date=[The first date to check for data, in yyyy-mm-dd format. If not given, it will default to a sensible value, depending on the value of --met-key]:START_DATE: ' \
'-e+[The last date (exclusive) to check for data, in yyyy-mm-dd format. If not given, it will default to a sensible value, depending on the value of --met-key]:END_DATE: ' \
'--end-date=[The last date (exclusive) to check for data, in yyyy-mm-dd format. If not given, it will default to a sensible value, depending on the value of --met-key]:END_DATE: ' \
'-m+[The key used in your TOML configuration file to declare a meteorology type. If you have \[\[data.download.geosit\]\] for example, then the key would be "geosit"]:MET_KEY: ' \
'--met=[The key used in your TOML configuration file to declare a meteorology type. If you have \[\[data.download.geosit\]\] for example, then the key would be "geosit"]:MET_KEY: ' \
'-i[Whether to ignore the default met types for different date ranges defined in the configuration]' \
'--ignore-defaults[Whether to ignore the default met types for different date ranges defined in the configuration]' \
'-d[Set this flag to print what would be downloaded, but not actually download anything]' \
'--dry-run[Set this flag to print what would be downloaded, but not actually download anything]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(jobs)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__jobs_commands" \
"*::: :->jobs" \
&& ret=0

    case $state in
    (jobs)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-jobs-command-$line[1]:"
        case $line[1] in
            (add)
_arguments "${_arguments_options[@]}" \
'--mod-fmt=[What format to output the .mod files in ("none" or "text"). Default is "text"]:MOD_FMT: ' \
'--vmr-fmt=[What format to output the .vmr files in ("none" or "text"). Default is "text"]:VMR_FMT: ' \
'--map-fmt=[What format to output the .map files in ("none", "text", or "netcdf"). Default is "text"]:MAP_FMT: ' \
'-p+[Priority to give this job; higher will be run before jobs with lower values]:PRIORITY: ' \
'--priority=[Priority to give this job; higher will be run before jobs with lower values]:PRIORITY: ' \
'--queue=[Which queue to add the job to, if not given, then will use the submitted job queue defined in the config]:QUEUE: ' \
'--no-delete[Never delete the output files from this job]' \
'-t[Pack the output files from this job into a single tarball]' \
'--to-tarball[Pack the output files from this job into a single tarball]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':site-id -- The two-letter site IDs used to identify the output in this job. Pass multiple site IDs as a comma-separated list. If multiple lat/lons are given, the number of site IDs must be 1 or equal to the number of lat/lons. If lat/lons are not given, then these site IDs must be recognized as standard sites:' \
':start-date -- The first date to generate priors for (inclusive), in YYYY-MM-DD format:' \
':end-date -- The last date to generate priors for (exclusive), in YYYY-MM-DD format:' \
':email -- The email address to contact when the priors are ready:' \
'::lat -- The latitudes to generate priors for. May be omitted if all SITE_ID values are standard sites. Note that if a latitude is provided for any locations, it must be provided for ALL locations; there is no way to use the default standard site location for only some sites in a single submission. See help text for SITE_ID for information on the interaction between the number of site IDs and lat/lon coordinates:' \
'::lon -- The longitudes to generate priors for. Same caveats as latitudes apply, must have the same number of latitudes as longitudes:' \
&& ret=0
;;
(reset)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':id -- The job ID to reset:' \
&& ret=0
;;
(delete)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':id -- The job ID to delete:' \
&& ret=0
;;
(clean-errored)
_arguments "${_arguments_options[@]}" \
'-s+[The earliest submission date to delete, only jobs with a submission time of or after midnight of this date will have their output deleted]:NOT_BEFORE: ' \
'--not-before=[The earliest submission date to delete, only jobs with a submission time of or after midnight of this date will have their output deleted]:NOT_BEFORE: ' \
'-e+[The last (exclusive) date to delete, only jobs with a submission time before midnight on this date will have their output deleted]:NOT_AFTER: ' \
'--not-after=[The last (exclusive) date to delete, only jobs with a submission time before midnight on this date will have their output deleted]:NOT_AFTER: ' \
'-d[Do not actually delete output, only print which jobs'\'' output will be deleted]' \
'--dry-run[Do not actually delete output, only print which jobs'\'' output will be deleted]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(print)
_arguments "${_arguments_options[@]}" \
'*-j+[Limit to certain job IDs, repeat this argument to specify multiple job IDs]:JOB_ID: ' \
'*--job-id=[Limit to certain job IDs, repeat this argument to specify multiple job IDs]:JOB_ID: ' \
'-a+[Limit jobs to those submitted on or after this date]:SUBMITTED_AFTER: ' \
'--submitted-after=[Limit jobs to those submitted on or after this date]:SUBMITTED_AFTER: ' \
'-b+[Limit jobs to those submitted before this date]:SUBMITTED_BEFORE: ' \
'--submitted-before=[Limit jobs to those submitted before this date]:SUBMITTED_BEFORE: ' \
'-e+[Limit jobs to those submitted under this email. Use "NONE" to filter for jobs submitted without an email]:SUBMITTER_EMAIL: ' \
'--submitter-email=[Limit jobs to those submitted under this email. Use "NONE" to filter for jobs submitted without an email]:SUBMITTER_EMAIL: ' \
'-d[Print out details descriptions of all matching jobs, rather than a table. Note that this is the only way to get all the information about jobs; many fields are omitted from the table to keep its width reasonable]' \
'--details[Print out details descriptions of all matching jobs, rather than a table. Note that this is the only way to get all the information about jobs; many fields are omitted from the table to keep its width reasonable]' \
'--all[List all jobs meeting the other criteria, not just pending jobs]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(input-files)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__input-files_commands" \
"*::: :->input-files" \
&& ret=0

    case $state in
    (input-files)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-input-files-command-$line[1]:"
        case $line[1] in
            (parse)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::input-files -- Paths to input files to parse:' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(email)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__email_commands" \
"*::: :->email" \
&& ret=0

    case $state in
    (email)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-email-command-$line[1]:"
        case $line[1] in
            (submitters)
_arguments "${_arguments_options[@]}" \
'-s+[Subject line for the email]:SUBJECT: ' \
'--subject=[Subject line for the email]:SUBJECT: ' \
'-b+[The body of the email. For longer emails, you can use the --body-file argument instead]:BODY: ' \
'--body=[The body of the email. For longer emails, you can use the --body-file argument instead]:BODY: ' \
'-f+[Path to a file containing the body of the email. For short emails, you can use --body instead]:BODY_FILE: ' \
'--body-file=[Path to a file containing the body of the email. For short emails, you can use --body instead]:BODY_FILE: ' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':to -- Who to use as the "to" email address; all the past submitters will be blind carbon copied:' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(site-info)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__site-info_commands" \
"*::: :->site-info" \
&& ret=0

    case $state in
    (site-info)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-site-info-command-$line[1]:"
        case $line[1] in
            (add-site)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':site-id -- The two character ID for the new site:' \
':site-name -- The long, human-readable name for this site:' \
':site-type -- Whether this is a TCCON or EM27 site:' \
&& ret=0
;;
(edit)
_arguments "${_arguments_options[@]}" \
'--site-id=[A new two-letter ID for the site - must be unique among all sites]:NEW_SITE_ID: ' \
'--name=[If given, the new name to assign for this site]:SITE_NAME: ' \
'--type=[If given, the new type (TCCON or EM27) for this site]:SITE_TYPE: ' \
'--output=[If given, the new output structure ("FlatModVmr", "FlatAll", "TreeModVmr", or "TreeAll") for this site. The "Flat" structures will put all the files in the root of the tarball, while the "Tree" structure retain ginputs `fpit/xx/*` directory structure. The "ModVmr" options only keep the `.mod` and `.vmr` files, while the "All" structures include the `.map` files as well]:OUTPUT_STRUCTURE: ' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':site-id -- The current two-letter ID for the site:' \
&& ret=0
;;
(print)
_arguments "${_arguments_options[@]}" \
'-t+[Limit to only sites of a certain type]:SITE_TYPE: ' \
'--type=[Limit to only sites of a certain type]:SITE_TYPE: ' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(add-info)
_arguments "${_arguments_options[@]}" \
'-l+[A human-readable description of the site'\''s location, e.g. "Park Fall, WI, USA"]:LOCATION: ' \
'--location=[A human-readable description of the site'\''s location, e.g. "Park Fall, WI, USA"]:LOCATION: ' \
'-x+[The longitude of the site. Must be between -180 and +360 and will be rectified to be within -180 to +180. When giving a negative value, using the = format, i.e. `--longitude=-90` may work better than `--longitude -90`]:LONGITUDE: ' \
'--longitude=[The longitude of the site. Must be between -180 and +360 and will be rectified to be within -180 to +180. When giving a negative value, using the = format, i.e. `--longitude=-90` may work better than `--longitude -90`]:LONGITUDE: ' \
'-y+[The latitude of the site. Must be between -90 and +90. See note on longitude for entering negative values]:LATITUDE: ' \
'--latitude=[The latitude of the site. Must be between -90 and +90. See note on longitude for entering negative values]:LATITUDE: ' \
'-c+[An optional comment giving more information about this date range]:COMMENT: ' \
'--comment=[An optional comment giving more information about this date range]:COMMENT: ' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':site-id -- The two letter ID of the site:' \
':start-date -- The first date, in YYYY-MM-DD format, that this location applies:' \
'::end-date -- The final date (exclusive) in YYYY-MM-DD format, that this location applies. If not given, this location is assumed to have no end date:' \
&& ret=0
;;
(print-info)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':site-id -- The two-letter ID for the site to print information about:' \
&& ret=0
;;
(json)
_arguments "${_arguments_options[@]}" \
'-d+[Provide site information for which sites were active on a given date, rather than all information. By default, only sites which were active on this date are returned, but this can be modified by the --inactive flag]:DATE: ' \
'--date=[Provide site information for which sites were active on a given date, rather than all information. By default, only sites which were active on this date are returned, but this can be modified by the --inactive flag]:DATE: ' \
'-m[Return the JSON in minified format, rather than pretty-printed]' \
'--minified[Return the JSON in minified format, rather than pretty-printed]' \
'-i[Changes the behavior of --date such that the returned JSON includes a value for every site, even if it was not active on the given date. In that case, the site information closest in time to the given date is provided]' \
'--inactive[Changes the behavior of --date such that the returned JSON includes a value for every site, even if it was not active on the given date. In that case, the site information closest in time to the given date is provided]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':json-type -- Which type of JSON to return. "flat" will be a list with one entry per site time period. If the same site has multiple time periods (e.g. how Darwin moved slightly), there will be multiple elements in the list with the same site ID. "grouped" will be a map with one element per site ID, each time period will be in a list of maps in each element:' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(site-jobs)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__site-jobs_commands" \
"*::: :->site-jobs" \
&& ret=0

    case $state in
    (site-jobs)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-site-jobs-command-$line[1]:"
        case $line[1] in
            (update-table)
_arguments "${_arguments_options[@]}" \
'-b+[]:NOT_BEFORE: ' \
'--not-before=[]:NOT_BEFORE: ' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(add-jobs)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(tar-files)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(flag-for-regen)
_arguments "${_arguments_options[@]}" \
'*-s+[Site ID to flag. Can provide this argument multiple times to flag multiple sites. Either this or --all-sites is required, but cannot have both]:SITE_ID: ' \
'*--site-id=[Site ID to flag. Can provide this argument multiple times to flag multiple sites. Either this or --all-sites is required, but cannot have both]:SITE_ID: ' \
'--all-sites[Flag all sites for regen. Either this or --site-id is required, but cannot have both]' \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':start-date -- First date to flag:' \
'::end-date -- Date after the last date to flag; if not given, only START_DATE is flagged:' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(config)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__config_commands" \
"*::: :->config" \
&& ret=0

    case $state in
    (config)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-config-command-$line[1]:"
        case $line[1] in
            (gen-config)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':path -- Path to write the default TOML file as:' \
&& ret=0
;;
(debug-config)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(completions)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
":: :_tccon-priors-cli__completions_commands" \
"*::: :->completions" \
&& ret=0

    case $state in
    (completions)
        words=($line[1] "${words[@]}")
        (( CURRENT += 1 ))
        curcontext="${curcontext%:*:*}:tccon-priors-cli-completions-command-$line[1]:"
        case $line[1] in
            (generate)
_arguments "${_arguments_options[@]}" \
'-h[Print help information]' \
'--help[Print help information]' \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
':shell -- Which shell to generate for, options are "bash", "elvish", "fish", "powershell", and "zsh":' \
&& ret=0
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
;;
(help)
_arguments "${_arguments_options[@]}" \
'*-v[More output per occurrence]' \
'*--verbose[More output per occurrence]' \
'(-v --verbose)*-q[Less output per occurrence]' \
'(-v --verbose)*--quiet[Less output per occurrence]' \
'*::subcommand -- The subcommand whose help message to display:' \
&& ret=0
;;
        esac
    ;;
esac
}

(( $+functions[_tccon-priors-cli_commands] )) ||
_tccon-priors-cli_commands() {
    local commands; commands=(
'met:Manage meteorology downloads and database' \
'jobs:Manage ginput jobs' \
'input-files:Manage job input files' \
'email:Send bulk emails about the priors' \
'site-info:Manage definition of standard sites and their locations' \
'site-jobs:Manage jobs for the standard sites' \
'config:Generate or check a configuration' \
'completions:' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs__add_commands] )) ||
_tccon-priors-cli__jobs__add_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli jobs add commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__add-info_commands] )) ||
_tccon-priors-cli__site-info__add-info_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info add-info commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-jobs__add-jobs_commands] )) ||
_tccon-priors-cli__site-jobs__add-jobs_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-jobs add-jobs commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__add-site_commands] )) ||
_tccon-priors-cli__site-info__add-site_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info add-site commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__met__check_commands] )) ||
_tccon-priors-cli__met__check_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli met check commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs__clean-errored_commands] )) ||
_tccon-priors-cli__jobs__clean-errored_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli jobs clean-errored commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__completions_commands] )) ||
_tccon-priors-cli__completions_commands() {
    local commands; commands=(
'generate:Generate completions for a shell, printing to stdout' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli completions commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__config_commands] )) ||
_tccon-priors-cli__config_commands() {
    local commands; commands=(
'gen-config:Generate a default configuration file from the command line' \
'debug-config:Read the configuration file pointed to by the PRIOR_CONFIG_FILE environment variable and print the internal representation to the screen. (Useful for checking that a config file is being parsed as you expect.) If the PRIOR_CONFIG_FILE variable is not set, then the default configuration is displayed' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli config commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__config__debug-config_commands] )) ||
_tccon-priors-cli__config__debug-config_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli config debug-config commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs__delete_commands] )) ||
_tccon-priors-cli__jobs__delete_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli jobs delete commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__met__download-dates_commands] )) ||
_tccon-priors-cli__met__download-dates_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli met download-dates commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__met__download-missing_commands] )) ||
_tccon-priors-cli__met__download-missing_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli met download-missing commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__edit_commands] )) ||
_tccon-priors-cli__site-info__edit_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info edit commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__email_commands] )) ||
_tccon-priors-cli__email_commands() {
    local commands; commands=(
'submitters:Send an email to anyone who has previously submitted a job' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli email commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-jobs__flag-for-regen_commands] )) ||
_tccon-priors-cli__site-jobs__flag-for-regen_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-jobs flag-for-regen commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__config__gen-config_commands] )) ||
_tccon-priors-cli__config__gen-config_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli config gen-config commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__completions__generate_commands] )) ||
_tccon-priors-cli__completions__generate_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli completions generate commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__completions__help_commands] )) ||
_tccon-priors-cli__completions__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli completions help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__config__help_commands] )) ||
_tccon-priors-cli__config__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli config help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__email__help_commands] )) ||
_tccon-priors-cli__email__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli email help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__help_commands] )) ||
_tccon-priors-cli__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__input-files__help_commands] )) ||
_tccon-priors-cli__input-files__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli input-files help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs__help_commands] )) ||
_tccon-priors-cli__jobs__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli jobs help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__met__help_commands] )) ||
_tccon-priors-cli__met__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli met help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__help_commands] )) ||
_tccon-priors-cli__site-info__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-jobs__help_commands] )) ||
_tccon-priors-cli__site-jobs__help_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-jobs help commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__input-files_commands] )) ||
_tccon-priors-cli__input-files_commands() {
    local commands; commands=(
'parse:Manually parse specific input files' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli input-files commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs_commands] )) ||
_tccon-priors-cli__jobs_commands() {
    local commands; commands=(
'add:Add a new job to the database' \
'reset:Reset a job to pending, clearing any output' \
'delete:Delete a job, clearing any output' \
'clean-errored:Delete output from jobs that errored' \
'print:Print jobs in the database' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli jobs commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__json_commands] )) ||
_tccon-priors-cli__site-info__json_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info json commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__met_commands] )) ||
_tccon-priors-cli__met_commands() {
    local commands; commands=(
'check:Check whether the required model files are listed in the database for a range of dates' \
'download-dates:Download model files for a range of dates' \
'download-missing:Download missing model files' \
'rescan:Rescan model download directories and add new files to the database' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli met commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__input-files__parse_commands] )) ||
_tccon-priors-cli__input-files__parse_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli input-files parse commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs__print_commands] )) ||
_tccon-priors-cli__jobs__print_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli jobs print commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__print_commands] )) ||
_tccon-priors-cli__site-info__print_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info print commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info__print-info_commands] )) ||
_tccon-priors-cli__site-info__print-info_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-info print-info commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__met__rescan_commands] )) ||
_tccon-priors-cli__met__rescan_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli met rescan commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__jobs__reset_commands] )) ||
_tccon-priors-cli__jobs__reset_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli jobs reset commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-info_commands] )) ||
_tccon-priors-cli__site-info_commands() {
    local commands; commands=(
'add-site:Define a new standard site' \
'edit:Modify an existing standard site' \
'print:Print out a table of defined standard sites' \
'add-info:Add a new date range defining the location of a standard site' \
'print-info:Print currently defined location info for a given site' \
'json:Return a JSON string of information about standard sites' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli site-info commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-jobs_commands] )) ||
_tccon-priors-cli__site-jobs_commands() {
    local commands; commands=(
'update-table:Update the standard site jobs table: add rows for new site-days possible' \
'add-jobs:Add jobs to generate standard sites'\'' priors for days in need of priors for which met data is available' \
'tar-files:Collect completed standard site jobs outputs into the standard sites'\'' tar files' \
'flag-for-regen:Flag a range of dates for standard priors regeneration, either for all sites or a subset' \
'help:Print this message or the help of the given subcommand(s)' \
    )
    _describe -t commands 'tccon-priors-cli site-jobs commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__email__submitters_commands] )) ||
_tccon-priors-cli__email__submitters_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli email submitters commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-jobs__tar-files_commands] )) ||
_tccon-priors-cli__site-jobs__tar-files_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-jobs tar-files commands' commands "$@"
}
(( $+functions[_tccon-priors-cli__site-jobs__update-table_commands] )) ||
_tccon-priors-cli__site-jobs__update-table_commands() {
    local commands; commands=()
    _describe -t commands 'tccon-priors-cli site-jobs update-table commands' commands "$@"
}

_tccon-priors-cli "$@"
