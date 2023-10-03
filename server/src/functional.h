#pragma once

typedef struct {
    bool (*func)(any_value *value1, any_value *value2);
    any_value value1;
} closure;