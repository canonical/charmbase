# Copyright 2019 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

test: lint check-copyright
	@python3 -m unittest

check-copyright:
	@found=""; \
	 for f in $$( find . -name \*.py -not -empty -type f -print ); do \
	  if ! grep -q "^# Copyright" "$$f"; then \
	    if [ -z "$$found" ]; then \
	      echo "The following files are missing Copyright headers"; \
	      found=yes; \
	    fi; \
	    echo "$$f"; \
	  fi; \
	 done; \
	 if [ -n "$$found" ]; then \
	   exit 1; \
	 fi

lint:
	@autopep8 -r --aggressive --diff --exit-code .
	@flake8 --config=.flake8

.PHONY: lint test check-copyright
