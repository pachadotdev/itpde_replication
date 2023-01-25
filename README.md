
<!-- README.md is generated from README.Rmd. Please edit that file -->

# Replicating The International Trade and Production Database for Estimation (ITPD-E)

<!-- badges: start -->
<!-- badges: end -->

The goal of this repository is to help me understand what was done and
how was done in the article.

The year range is 1986-2020.

# Agriculture

## Trade data

### Sources

The Food and Agriculture Organization of the United Nations Statistics
Division ([FAOSTAT](https://www.fao.org/faostat/en/#data/TM)), which
gathers data from UNSD, Eurostat, and other national authorities as
needed. In particular
<https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip>.

### SOURCES NOT IN THE ARTICLE BUT REQUIRED ANYWAYS

World Integrated Trade Solutions
([WITS](https://wits.worldbank.org/product_concordance.html)). In
particular
<http://wits.worldbank.org/data/public/concordance/Concordance_H0_to_I3.zip>.

According to “Guidelines for the compilation of Food Balance Sheets”:

> The correspondence table for FCL to HS6 can be found at FAO, 2017,
> FAOSTAT commodity definitions and correspondences: FCL – HS 2007,
> available at:
> <http://www.fao.org/economic/ess/ess-standards/commodity/item-hs/en/>
> (accessed 22 February 2017), while the complementary table of
> correspondences for HS6 back to FCL is available at FAO, 2017, FAOSTAT
> commodity definitions and correspondences: HS 2007 – FCL,
> <http://www.fao.org/economic/ess/ess-standards/commodity/hs-item/en/>
> (accessed 22 February 2017).

These links are broken as of 2021-12-20

### Special cases

From the article:

> FAOSTAT does not include international trade data for FCL item 27
> “rice (paddy)”. Instead, we use data from the UN Commodity Trade
> Statistics Database (COMTRADE), HS sector 100,610 “Cereals; rice in
> the husk (paddy or rough)”

> …we classify all industries between 1500 and 1601 of ISIC rev. 3 as
> manufacturing industries.

## Production data

Source: FAOSTAT (Value of Agricultural Production). I used production in
current dollars, NOT in constant dollars of 2014-2016.

Also we need UN COMTRADE data here to match country numeric ID to ISO-3
codes. Not all ID-Country pairs match (i.e., France, Italy, etc)

# Fishing and agriculture

From the WP for version 2:

> The IMF exchange rateseries for Belarus is smooth around 2015-16 and
> does not exhibit a jump by a factor of 10. In order to ensure
> consistency of the value of trade flows in R02, output data for
> Belarus have been discarded. This issue also afflicts the R01 services
> trade data and an erratum to this effect will be published/

From the article:

> Production data from the UN National Accounts requires two principal
> modiﬁcations before it can be merged with trade flow data: ﬁrst, gross
> output data as obtained from the UN Statistical Division are
> denominated in local currency units. Since the conversion of services
> production statistics should ideally use the same USD exchange rates
> that are used for services trade flow data, we apply the average
> annual exchanges rates from the IMF’s International Financial
> Statistics series.”

Also from the article:

> The UN data show that some countries report only in the former, some
> only in the latter, and some countries report in both frameworks, at
> least for some intermittent years as their reporting transitions from
> SNA 1993 to SNA 2008. We blend data from both SNA frameworks so as to
> maximize coverage in terms of countries and industries.

## Trade data

### Sources

UN COMTRADE: HS rev 1992.

## Production data

### Sources

UN Data: Table 2.6 Output, gross value added and fixed assets by
industries at current prices (ISIC Rev.4) from the SNA

IMF: Domestic Currency per U.S. Dollar, Period Average, Rate (I assumed
this is what the article used)

# Download dates

- UN COMTRADE data 2022-12-29
- FAO 2022-10-24
- UNIDO 2022-12-13
- UN Data 2023-01-23 for Table 2.6 Output, gross value added and fixed
  assets by industries at current prices (ISIC Rev.4)
- IMF 2022-01-23 for Domestic Currency per U.S. Dollar, Period Average,
  Rate
