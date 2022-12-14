# tap-podbean

`tap-podbean` is a Singer tap for Podbean.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps and the [Podbean API Reference](https://developers.podbean.com/podbean-api-docs/)

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install tap-podbean
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-podbean.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-podbean --about --format=markdown
```
-->

Podbean tap class.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| client_id           | True     | None    | The client identifier to authenticate against the API service. |
| client_secret       | True     | None    | The client secret to authenticate against the API service. |
| start_date          | True     | None    | The earliest datetime (UTC) to sync records. |
| auth_expiration     | False    |     300 | Expiraton in seconds for auth. (Range: 60-604800) |
| limit               | False    |     100 | The number of records to return per page. (Range: 0-100) |
| user_agent          | False    | None    | User agent to present to the API. |
| api_url             | False    | None    | Override the API service base URL. |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-podbean --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

Obtain the client_id and client_secret from an existing app registration or [register a new app](https://developers.podbean.com/).

## Usage

You can easily run `tap-podbean` by itself or in a pipeline using [Meltano](https://meltano.com/).

Scopes used by this tap:

* podcast_read
* episode_read
* private_members

### Executing the Tap Directly

```bash
tap-podbean --version
tap-podbean --help
tap-podbean --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_podbean/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-podbean` CLI interface directly using `poetry run`:

```bash
poetry run tap-podbean --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-podbean
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-podbean --version
# OR run a test `elt` pipeline:
meltano elt tap-podbean target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
