import { Component } from 'react'
import type { ErrorInfo, ReactNode } from 'react'

interface Props {
  children: ReactNode
  resetKey: string
}

export class AppErrorBoundary extends Component<Props, { error: Error | null }> {
  state: { error: Error | null } = { error: null }

  static getDerivedStateFromError(error: Error) {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('The web UI could not render', error, info)
  }

  componentDidUpdate(previousProps: Props) {
    if (this.state.error && previousProps.resetKey !== this.props.resetKey) {
      this.setState({ error: null })
    }
  }

  render() {
    if (!this.state.error) return this.props.children
    return (
      <div className="error-state" role="alert">
        <span className="eyebrow">THE PAGE HIT AN ERROR</span>
        <strong>{this.state.error.message}</strong>
        <button className="button button-coral" type="button" onClick={() => window.location.reload()}>RELOAD APP</button>
      </div>
    )
  }
}
