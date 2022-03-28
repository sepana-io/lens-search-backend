from typing import List


def get_profiles_query(user_ids: List[str]) -> str:
    return """query Profiles {
    profiles(request: { profileIds: USER_IDS, limit: 50 }) {
        items {
        id
        name
        bio
        location
        website
        twitterUrl
        picture {
            ... on NftImage {
            contractAddress
            tokenId
            uri
            verified
            }
            ... on MediaSet {
            original {
                url
                mimeType
            }
            }
            __typename
        }
        handle
        coverPicture {
            ... on NftImage {
            contractAddress
            tokenId
            uri
            verified
            }
            ... on MediaSet {
            original {
                url
                mimeType
            }
            }
            __typename
        }
        ownedBy
        depatcher {
            address
            canUseRelay
        }
        stats {
            totalFollowers
            totalFollowing
            totalPosts
            totalComments
            totalMirrors
            totalPublications
            totalCollects
        }
        followModule {
            ... on FeeFollowModuleSettings {
            type
            amount {
                asset {
                symbol
                name
                decimals
                address
                }
                value
            }
            recipient
            }
            __typename
        }
        }
        pageInfo {
        prev
        next
        totalCount
        }
    }
    }
    """.replace('USER_IDS', str(user_ids).replace("'", '"'))


def get_followers_query(user_id: str) -> str:

    return """query Followers {
        followers(request: {
            profileId: "USER_ID",
            limit: 50
            }) {
                items {
                    wallet {
                        address
                        defaultProfile {
                            id
                            }
                        }
                    }
                }
        }""".replace('USER_ID', user_id)