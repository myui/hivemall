<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# How to build

#### Prerequisites

* Maven 3.x
* JDK 1.7 or later

```
$ cd hivemall

# This is a workaround for resolving xgboost dependencies.
$ mvn validate -Pxgboost
 
$ mvn clean package
```

# Contribution guideline

This guide documents the best way to make various types of contribution to Apache Hivemall, 
including what is required before submitting a code change.

Contributing to Hivemall doesn't just mean writing code. Helping new users on the [mailing list](/mail-lists.html), 
testing releases, and improving documentation are also welcome. In fact, proposing significant code changes usually 
requires first gaining experience and credibility within the community by helping in other ways. This is also a guide 
to becoming an effective contributor. So, this guide organizes contributions in order that they should probably be 
considered by new contributors who intend to get involved long-term. Build some track record of helping others, 
rather than just open pull requests.

## Preparing to contribute code changes

Before proceeding, contributors should evaluate if the proposed change is likely to be relevant, new and actionable:

* Is it clear that code must change? Proposing a [JIRA](https://issues.apache.org/jira/browse/HIVEMALL) and pull request is appropriate only when a clear problem or 
change has been identified. If simply having trouble using Hivemall, use the [mailing lists](/mail-lists.html) first, 
rather than consider filing a JIRA or proposing a change. When in doubt, email user@hivemall.incubator.apache.org first 
about the possible change
* Search the user@hivemall.incubator.apache.org and dev@hivemall.incubator.apache.org mailing list archives for related 
discussions. Often, the problem has been discussed before, with a resolution that doesn't require a code change, or recording 
what kinds of changes will not be accepted as a resolution.
* Search JIRA for existing issues: https://issues.apache.org/jira/browse/HIVEMALL . Please search by typing keyword in search box.

**When you contribute code, you affirm that the contribution is your original work and that you license the work to the project 
under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via 
pull request, email, or other means you agree to license the material under the project's open source license and warrant that 
you have the legal authority to do so.**

### JIRA

Generally, Hivemall uses JIRA to track logical issues, including bugs and improvements, and uses Github pull requests to 
manage the review and merge of specific code changes. That is, JIRAs are used to describe what should be fixed or changed, 
and high-level approaches, and pull requests describe how to implement that change in the project's source code. 
For example, major design decisions are discussed in JIRA.

1. Find the existing [Hivemall](https://issues.apache.org/jira/browse/HIVEMALL) JIRA that the change pertains to.
    * Do not create a new JIRA if creating a change to address an existing issue in JIRA; add to the existing discussion 
and work instead 
    * Look for existing pull requests that are linked from the JIRA, to understand if someone is already working on the JIRA 
2. If the change is new, then it usually needs a new JIRA. However, trivial changes, where the what should change is virtually the same as the how it should change do not require a JIRA. Example: "Fix typos in Foo scaladoc"
3. If required, create a new JIRA:
    * Provide a descriptive Title. "Update web UI" or "Problem in scheduler" is not sufficient. "Inject the Hivemall aggregate functionality in RelationalGroupedDataset" is good.
    * Write a detailed Description. For bug reports, this should ideally include a short reproduction of the problem. For new features, it may include a design document.
    * Set required fields:
        * *Issue Type*. Generally, Bug, Improvement and Test are the only types used in Hivemall.
        * *Affects Version*. For Bugs, assign at least one version that is known to exhibit the problem or need the change
        * Do not set the following fields:
            * *Fix Version*. This is assigned by committers only when resolved.
            * *Target Version*. This is assigned by committers to indicate a PR has been accepted for possible fix by the target version.
4. If the change is a large change, consider inviting discussion on the issue at dev@hivemall.incubator.apache.org first before proceeding to implement the change.

### Pull Request
1. Fork the Github repository at http://github.com/apache/incubator-hivemall if you haven't already
2. Clone your fork, create a new branch, push commits to the branch.
3. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed.
4. Run all tests with `mvn test` to verify that the code still compiles, passes tests, and passes style checks.
5. Open a pull request against the master branch of [apache/incubator-hivemall](https://github.com/apache/incubator-hivemall). (Only in special cases would the PR be opened against other branches.)
    * The PR title should be of the form [HIVEMALL-xxxx]  Title, where HIVEMALL-xxxx is the relevant JIRA number, Title may be the JIRA's title or a more specific title describing the PR itself.
    * If the pull request is still a work in progress, and so is not ready to be merged, but needs to be pushed to Github to facilitate review, then add [WIP] head of the title.
    * Consider identifying committers or other contributors who have worked on the code being changed. Find the file(s) in Github and click "Blame" to see a line-by-line annotation of who changed the code last. You can add @username in the PR description to ping them immediately.
    * Please state that the contribution is your original work and that you license the work to the project under the project's open source license.
    * The related JIRA, if any, will be marked as "In Progress" and your pull request will automatically be linked to it. There is no need to be the Assignee of the JIRA to work on it, though you are welcome to comment that you have begun work.
6. The Jenkins automatic pull request builder will test your changes
7. After about some minutes, Jenkins will post the results of the test to the pull request, along with a link to the full results on Jenkins.
8. Watch for the results, and investigate and fix failures promptly
    * Fixes can simply be pushed to the same branch from which you opened your pull request
    * Jenkins will automatically re-test when new commits are pushed
    * If the tests failed for reasons unrelated to the change (e.g. Jenkins outage), then a committer can request a re-test with "Jenkins, retest this please". Ask if you need a test restarted.
 
## The Review Process
* Other reviewers, including committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch. 
* Lively, polite, rapid technical debate is encouraged from everyone in the community. The outcome may be a rejection of the entire change. 
* Reviewers can indicate that a change looks suitable for merging with a comment such as: "I think this patch looks good". Hivemall uses the LGTM convention for indicating 
the strongest level of technical sign-off on a patch: simply comment with the word "LGTM". It specifically means: 
"I've looked at this thoroughly and take as much ownership as if I wrote the patch myself". If you comment LGTM you 
will be expected to help with bugs or follow-up issues on the patch. Consistent, judicious use of LGTMs is a great 
way to gain credibility as a reviewer with the broader community. 
* Sometimes, other changes will be merged which conflict 
with your pull request's changes. The PR can't be merged until the conflict is resolved. This can be resolved with 
"git fetch origin" followed by "git merge origin/master" and resolving the conflicts by hand, then pushing the result 
to your branch. 
* Try to be responsive to the discussion rather than let days pass between replies

## Closing your pull request / JIRA

* If a change is accepted, it will be merged and the pull request will automatically be closed, along with the associated JIRA if any
    * Note that in the rare case you are asked to open a pull request against a branch besides master, that you will actually have to close the pull request manually
    * The JIRA will be Assigned to the primary contributor to the change as a way of giving credit. If the JIRA isn't closed and/or Assigned promptly, comment on the JIRA.
* If your pull request is ultimately rejected, please close it promptly
* If a pull request has gotten little or no attention, consider improving the description or the change itself and ping likely reviewers again after a few days. Consider proposing a change that's easier to include, like a smaller and/or less invasive change.
* If it has been reviewed but not taken up after weeks, after soliciting review from the most relevant reviewers, or, has met with neutral reactions, the outcome may be considered a "soft no". It is helpful to withdraw and close the PR in this case.
* If a pull request is closed because it is deemed not the right approach to resolve a JIRA, then leave the JIRA open. However if the review makes it clear that the issue identified in the JIRA is not going to be resolved by any pull request (not a problem, won't fix) then also resolve the JIRA.



[Here](http://www.apache.org/foundation/getinvolved.html) is a general guide for contributing to Apache Project.

