import {
  type GatewayState,
  releasePayment,
  releasePaymentAfterFailure,
  settleAndRecord,
} from '../dispatch'
import { recoverPayment as recoverDurablePayment } from '../payment-recovery-worker'
import type { GatewayConfig } from '../types'
import { releaseTaskPayment } from './payment-recovery'
import type { PaymentRecoveryDependencies } from './payment-recovery'
import type { TaskFinalizationDependencies } from './task-finalization'
import type { Task } from './types'
import type { TaskStateStore } from './task-state'

export interface TaskLifecycleDependencies {
  taskStore: TaskStateStore
  config: GatewayConfig
  state: GatewayState
  deliverPush: (task: Task) => Promise<void>
}

export interface TaskLifecycle {
  payment: PaymentRecoveryDependencies
  finalization: TaskFinalizationDependencies
}

export function createTaskLifecycle(deps: TaskLifecycleDependencies): TaskLifecycle {
  const payment: PaymentRecoveryDependencies = {
    taskStore: deps.taskStore,
    paymentOperations: deps.config.x402.paymentOperations,
    paymentRecovery: deps.config.paymentRecovery,
    releasePayment: (authz, reason) => releasePayment(authz, deps.config, reason),
    releasePaymentAfterFailure: (authz, reason, workObserved) =>
      releasePaymentAfterFailure(authz, deps.config, reason, workObserved),
    recoverDurablePayment: (recoveryId, options) =>
      recoverDurablePayment(recoveryId, deps.config, options),
    deliverPush: deps.deliverPush,
  }
  const finalization: TaskFinalizationDependencies = {
    taskStore: deps.taskStore,
    settle: (authz, usage, options) =>
      settleAndRecord(authz.agent, authz, usage, deps.config, deps.state.obs, options),
    resolveAgent: deps.config.resolveAgent,
    paymentOperations: deps.config.x402.paymentOperations,
    paymentRecovery: deps.config.paymentRecovery,
    recoverDurablePayment: (recoveryId, options) =>
      recoverDurablePayment(recoveryId, deps.config, options),
    releasePaymentAfterFailure: payment.releasePaymentAfterFailure,
    releaseTaskPayment: (authz, task, reason, workObserved) =>
      releaseTaskPayment(authz, task, payment, reason, workObserved),
    deliverPush: deps.deliverPush,
  }
  return { payment, finalization }
}
