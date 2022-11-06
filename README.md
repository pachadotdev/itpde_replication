
<!-- README.md is generated from README.Rmd. Please edit that file -->

# Replicating The International Trade and Production Database for Estimation (ITPD-E)

<!-- badges: start -->
<!-- badges: end -->

The goal of this repository is to help me understand what was done and
how was done in the article.

There are still many things I don’t understand, starting by converting
HS to ISIC codes, to start with.

In the meanwhile I scraped some tables from the PDF.

# Agriculture

## Final goal

## Time range

1986-2020

## Trade data

### Sources

The Food and Agriculture Organization of the United Nations Statistics
Division ([FAOSTAT](https://www.fao.org/faostat/en/#data/TM)), which
gathers data from UNSD, Eurostat, and other national authorities as
needed. In particular
<https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip>.

*SOURCES NOT IN THE ARTICLE BUT REQUIRED ANYWAYS*

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

The links are broken :(

I already asked on Twitter
(<https://twitter.com/pachadotdev/status/1584276785868734464>) and
emailed the authors.

### Special cases

From the article:

> FAOSTAT does not include international trade data for FCL item 27
> “rice (paddy)”. Instead, we use data from the UN Commodity Trade
> Statistics Database (COMTRADE), HS sector 100,610 “Cereals; rice in
> the husk (paddy or rough)”

> …we classify all industries between 1500 and 1601 of ISIC rev. 3 as
> manufacturing indus- tries.

## Production data

Also FAOSTAT. Use the data on the “Value of Agricultural Production”.

For production obtained from UN DATA (i.e. Forestry and Fishing TODO ADD
ANY OTHER CASE), the production is provided in each country’s currency.
Therefore I downloaded the GDP in current prices in local currency
(<https://unstats.un.org/unsd/amaapi/api/file/1>) and US dollar
(<https://unstats.un.org/unsd/amaapi/api/file/2>). Then I obtained a
constant to multiply each production value to express all in dollars.

There are inconsistencies in UN datasets (i.e., “U.R. of Tanzania:
Mainland” versus “Tanzania - Mainland” depending if we use UN Data or UN
Stat)

Also we need UN COMTRADE data here to match country numeric ID to ISO-3
codes. Not all ID-Country pairs match (i.e., France, Italy, etc)

## Download dates

UN COMTRADE data 2022-10-31 FAO 2022-10-24
