import type { Task } from './types'

const PAYMENT_RECOVERY_KEYS = [
  'gatewayFinalizing',
  'gatewayPaymentRelease',
  'gatewayPaymentRecovery',
] as const

export function hasPendingPaymentRecovery(task: Task): boolean {
  return PAYMENT_RECOVERY_KEYS.some((key) => task.metadata?.[key] !== undefined)
}
