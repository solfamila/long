#include "runtime_qos.h"

#if defined(__APPLE__)
#include <pthread.h>
#include <pthread/qos.h>
#endif

#include <string>

namespace runtime_qos {

namespace {

#if defined(__APPLE__)
void applyThreadName(std::string_view label) {
    if (label.empty()) {
        return;
    }
    std::string truncated(label.substr(0, 63));
    pthread_setname_np(truncated.c_str());
}

void applyThreadQos(std::string_view qosName) {
    qos_class_t qosClass = QOS_CLASS_DEFAULT;
    if (qosName == "userInteractive") {
        qosClass = QOS_CLASS_USER_INTERACTIVE;
    } else if (qosName == "userInitiated") {
        qosClass = QOS_CLASS_USER_INITIATED;
    } else if (qosName == "utility") {
        qosClass = QOS_CLASS_UTILITY;
    } else if (qosName == "background") {
        qosClass = QOS_CLASS_BACKGROUND;
    } else {
        return;
    }
    pthread_set_qos_class_self_np(qosClass, 0);
}
#endif

} // namespace

void applyCurrentThreadSpec(runtime_registry::QueueId queueId) {
#if defined(__APPLE__)
    const auto spec = runtime_registry::queueSpec(queueId);
    applyThreadName(spec.label);
    applyThreadQos(spec.qosName);
#else
    (void)queueId;
#endif
}

} // namespace runtime_qos
