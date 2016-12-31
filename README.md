A tool for parsing data from log files. It operates by tailing a log file and then parsing each line for fields.

Fields are defined in the config file or the command line. Command line ones override the config file.

Field Definitions are like this:
``` json
{
  "position": 1,
  "type": "string",
  "label": "overiding the value",
  "delimiter": "-",
  "required": true
}
```

only the `position` is required. The defaults are like this:

- delimiter: "="
- type: "string"
- label: the key from the split token
- required: false

the only supported types are `number`, `float`, `string`, and `bool`. Others will cause a warning and just default to `string`.
If there is no label specified the value from the split token will be used. Any errors when parsing will be reported, but the value will just be ignored unless `required` is set.

To specify some values on the command line you can use this format:

```
[!]position[:label[:type]]
```

the `!` indicates required. The delimiter used is '='. To override that (for all command line values) specify the `-d`  flag.
