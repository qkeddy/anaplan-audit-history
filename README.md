# Anaplan Python Operations
Anaplan administrative operations developed for Python

![badmath](https://img.shields.io/github/license/qkeddy/anaplan-python-ops)
![badmath](https://img.shields.io/github/issues/qkeddy/anaplan-python-ops)
![badmath](https://img.shields.io/github/languages/top/qkeddy/anaplan-python-ops)
![badmath](https://img.shields.io/github/watchers/qkeddy/anaplan-python-ops)
![badmath](https://img.shields.io/github/forks/qkeddy/anaplan-python-ops)

## Description
Demonstrates using the Anaplan REST API with OAuth with device-based authorization. The code highlights how to generate a `device_id`, `access_token`, and `refresh_token`. Additionally, the code highlights a multi-threaded approach to request a new `access_token` while performing other longer running operations such as a large data load. Please note that with this code example, the concept is simulated by calling ***Get Workspaces*** multiple times. 

A link to the GitHub repository can be viewed [here](https://github.com/qkeddy/anaplan-python-ops).

## Table of Contents

- [Deployment](#deployment)
- [Features](#features)
- [Usage](#usage)
- [Tests](#tests)
- [Credits](#credits)
- [License](#license)
- [How to Contribute](#how-to-contribute)

## Deployment
1. Fork and clone project repo
2. Using `pip install`, download and install the following Python libraries
`sys`, 
`logging`, 
`threading`, 
`requests`,
`json`,
`time`,
`pyjwt`, and
`apsw`
3. Create a device authorization code grant (known as a device grant in Anaplan). More information is available [here](https://help.anaplan.com/2ef7b883-fe87-4194-b028-ef6e7bbf8e31-OAuth2-API). 



## Features
- Dynamically creates a new `access_token` using a `refresh_token` on an independent worker thread.
- Secure storage of tokens

## Usage

1. When executing the first time on a particular device, open the CLI in the project folder and run `python3 anaplan.py -r -c <<enter Client ID>>` 

2. After the above step, the script can be executed unattended by simply executing `python3 anaplan.py`

Note: The `client_id` and `refresh_token` are stored as encrypted values in a SQLite database.

## Tests
Currently, no automated unit tests have been built. 

## Credits
- [Quinlan Eddy](https://github.com/qkeddy)

## License
MIT License

Copyright (c) 2022 Quin Eddy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.



## How to Contribute

If you would like to contribute to this project. Please email me at qkeddy@gmail.com. If you would like to contribute to future projects, please follow me at https://github.com/qkeddy.

It is requested that all contributors adhere to the standards outlined in the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).