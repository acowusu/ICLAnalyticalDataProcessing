#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>

#ifdef _WIN32
extern "C" {
__declspec(dllexport) BOSSExpression* evaluate(BOSSExpression* e);
__declspec(dllexport) void reset();
}
#endif // _WIN32
