"""{
  reserves(orderBy: underlyingAsset) {
    underlyingAsset
    symbol
    name
    decimals
    paramsHistory {
      liquidityIndex
      timestamp
    }
    id
  }
}"""
