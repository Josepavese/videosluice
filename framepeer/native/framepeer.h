#ifndef FRAMEPEER_NATIVE_CONTRACT_H
#define FRAMEPEER_NATIVE_CONTRACT_H
/* Semantic results shared by native providers. No OS or application policy. */
enum {
    FRAMEPEER_OK = 0,
    FRAMEPEER_INVALID = -1,
    FRAMEPEER_UNAVAILABLE = -2,
    FRAMEPEER_UNAUTHORIZED = -3,
    FRAMEPEER_RETIRED = -4,
    FRAMEPEER_DEADLINE = -5,
    FRAMEPEER_RECORD = -6
};
#endif
