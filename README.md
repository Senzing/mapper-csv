# senzing-repository-template

This repository contains exemplar artifacts (files) that can be used in other Senzing repositories.

GitHub provides support for these artifacts.  As an example, click on the following links on this GitHub repository:

- "Insights" tab > "[Community](https://github.com/senzing/senzing-repository-template/community)" on left-hand navigation bar

## Files

Consider copy-paste-modifying these files into your Senzing repository:

1. [CODE\_OF\_CONDUCT.md](#code_of_conductmd)
1. [CONTRIBUTING.md](#contributingmd)
1. .github/
    1. [senzing-corporate-contributor-license-agreement.pdf](#githubsenzing-corporate-contributor-license-agreementpdf)
    1. [senzing-individual-contributor-license-agreement.pdf](#githubsenzing-individual-contributor-license-agreementpdf)
    1. ISSUE\_TEMPLATE/
        1. [bug\_report.md](#githubissue_templatebug_reportmd)
        1. [feature\_request.md](#githubissue_templatefeature_requestmd)
1. [LICENSE](#license)
1. [PULL\_REQUEST\_TEMPLATE.md](#pull_request_templatemd)
1. [README.md](#readmemd)

## README.md

Although the file you are reading is a `README.md` file, this isn't the style of `README.md` for most projects.
Depending upon the type of repository, the following `README.md` templates may be more appropriate:  

1. [README.md](.github/README_TEMPLATE/demonstration/README.md) template for demonstrations. Examples:
    1. [hello-senzing-ibm-node-red/README.md](https://github.com/senzing/hello-senzing-ibm-node-red/blob/master/README.md)
    1. [hello-senzing-springboot-java/README.md](https://github.com/senzing/hello-senzing-springboot-java/blob/master/README.md)

## LICENSE

The `LICENSE` file describes the terms and conditions under which the code in the repository can be used.
The recommended license file is
"[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)".
A comparison of licenses can be found at
[choosealicense.com](https://choosealicense.com/licenses/).

The [LICENSE](LICENSE) file in this repository is based on
"[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)".

### How to create LICENSE

1. Option #1: Using GitHub's "Wizard"
    1. When [creating a new repository](https://github.com/new), in the "Add a license:" drop-down, choose "Apache License 2.0"
1. Option #2: Manual file creation
    1. See GitHub's [Adding a license to a repository](https://help.github.com/articles/adding-a-license-to-a-repository/)

## CODE\_OF\_CONDUCT.md

The `CODE_OF_CONDUCT.md` file describes the social conventions among contributors to the repository.

> A *code of conduct* defines standards for how to engage in a community. It signals an inclusive environment that respects all contributions. It also outlines procedures for addressing problems between members of your project's community. For more information on why a code of conduct defines standards and expectations for how to engage in a community, see the [Open Source Guide](https://opensource.guide/code-of-conduct/).
>
> -- <cite>GitHub's [Adding a code of conduct to your project](https://help.github.com/articles/adding-a-code-of-conduct-to-your-project/)</cite>

The [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) file in this repository is based on GitHub's "[Contributor Covenant](https://www.contributor-covenant.org/version/1/4/code-of-conduct.html)".

### How to create CODE\_OF\_CONDUCT.md

1. Option #1: Using GitHub's "Wizard"
    1. [github.com](https://github.com/) > (choose repository) > Insights > Community > Code of conduct > "Add" button > "Contributor Covenant"
1. Option #2: Manual file creation
    1. See GitHub's [Adding a code of conduct to your project](https://help.github.com/articles/adding-a-code-of-conduct-to-your-project/)
    1. Alternative `CODE_OF_CONDUCT.md` content:
        1. [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html)

## CONTRIBUTING.md

The `CONTRIBUTING.md` file describes the process for contributing to the repository.

> To help your project contributors do good work, you can add a file with contribution guidelines to your project repository's root. When someone opens a pull request or creates an issue, they will see a link to that file.
>
> -- <cite>GitHub's [Setting guidelines for repository contributors](https://help.github.com/articles/setting-guidelines-for-repository-contributors/)</cite>

The [CONTRIBUTING.md](CONTRIBUTING.md) file in this repository is an example that needs to be modified to represent the requirements of the actual repository.

### How to create CONTRIBUTING.md

1. Option #1: Using GitHub's "Wizard"
    1. [github.com](https://github.com/) > (choose repository) > Insights > Community > Contributing > "Add" button
1. Option #2: Manual file creation
    1. See GitHub's [Setting guidelines for repository contributors](https://help.github.com/articles/setting-guidelines-for-repository-contributors/)

## PULL\_REQUEST\_TEMPLATE.md

The `PULL_REQUEST_TEMPLATE.md` file asks a pull requester for information about the pull request.

> When you add a pull request template to your repository, project contributors will automatically see the template's contents in the pull request body.
>
> -- <cite>GitHub's [Creating a pull request template for your repository](https://help.github.com/articles/creating-a-pull-request-template-for-your-repository/)</cite>

The [PULL_REQUEST_TEMPLATE.md](PULL_REQUEST_TEMPLATE.md) file in this repository
is an example that can be modified.

### How to create PULL\_REQUEST\_TEMPLATE.md

1. Option #1: Using GitHub's "Wizard"
    1. [github.com](https://github.com/) > (choose repository) > Insights > Community > Pull request template > "Add" button
1. Option #2: Manual file creation
    1. See GitHub's [Creating a pull request template for your repository](https://help.github.com/articles/creating-a-pull-request-template-for-your-repository/)

## .github/senzing-corporate-contributor-license-agreement.pdf

The Senzing, INC. Software Grant and Corporate Contributor License Agreement (CCLA),
[corporate-contributor-license-agreement.pdf](.github/senzing-corporate-contributor-license-agreement.pdf),
is the standard agreement for a corporation's contribution to a Senzing repository.

### How to create .github/senzing-corporate-contributor-license-agreement.pdf

1. Make a `.github` directory in the repository
1. Copy [senzing-corporate-contributor-license-agreement.pdf](.github/senzing-corporate-contributor-license-agreement.pdf) into the new `.github` directory
1. *DO NOT* modify the contents of [senzing-corporate-contributor-license-agreement.pdf](.github/senzing-corporate-contributor-license-agreement.pdf) without legal approval.
1. Reference `senzing-corporate-contributor-license-agreement.pdf` in [CONTRIBUTING.md](#contributingmd)

## .github/senzing-individual-contributor-license-agreement.pdf

The Individual Contributor License Agreement (ICLA),
[senzing-individual-contributor-license-agreement.pdf](.github/senzing-individual-contributor-license-agreement.pdf),
is the standard agreement for an individual's contribution to a Senzing repository.
*Note:* if an individual is contributing on behalf of a company, the
[senzing-corporate-contributor-license-agreement.pdf](#githubsenzing-corporate-contributor-license-agreementpdf)
must also be submitted and accepted.

### How to create .github/senzing-individual-contributor-license-agreement.pdf

1. Make a `.github` directory in the repository
1. Copy [senzing-individual-contributor-license-agreement.pdf](.github/senzing-individual-contributor-license-agreement.pdf) into the new `.github` directory
1. *DO NOT* modify the contents of [senzing-individual-contributor-license-agreement.pdf](.github/senzing-individual-contributor-license-agreement.pdf) without legal approval.
1. Reference `senzing-individual-contributor-license-agreement.pdf` in [CONTRIBUTING.md](#contributingmd)

## .github/ISSUE\_TEMPLATE/bug\_report.md

A template presented to the Contributor when creating an issue that reports a bug.

The [bug_report.md](.github/ISSUE_TEMPLATE/bug_report.md) file in this repository
is an example that can be modified.

### How to create .github/ISSUE\_TEMPLATE/bug\_report.md

1. Option #1: Using GitHub's "Wizard"
    1. [github.com](https://github.com/) > (choose repository) > Insights > Community > Issue templates > "Add" button > Add template: Bug report

## .github/ISSUE\_TEMPLATE/feature\_request.md

A template presented to the Contributor when creating an issue that requests a feature.

The [feature_request.md](.github/ISSUE_TEMPLATE/feature_request.md) file in this repository
is an example that can be modified.

### How to create .github/ISSUE\_TEMPLATE/feature\_request.md

1. Option #1: Using GitHub's "Wizard"
    1. [github.com](https://github.com/) > (choose repository) > Insights > Community > Issue templates > "Add" button > Add template: Feature request