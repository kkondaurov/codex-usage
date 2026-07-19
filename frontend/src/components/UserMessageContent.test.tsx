import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { UserMessageContent } from './UserMessageContent'

describe('UserMessageContent', () => {
  it('keeps an ordinary request as the only reading surface', () => {
    render(<UserMessageContent raw={'Please compare both options.\n\n- Cost\n- Reliability'} fallback="Please compare both options." />)

    expect(screen.getByText('Please compare both options.')).toBeVisible()
    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual(['Cost', 'Reliability'])
    expect(screen.queryByRole('region', { name: 'Supporting material' })).not.toBeInTheDocument()
  })

  it('leads with the full authored request and folds an app capture beneath it', () => {
    const raw = `# Applications mentioned by the user:

<appshot app="Google Chrome" window-title="Codex usage">
Window: "Codex usage", App: Google Chrome.
large accessibility capture
</appshot>

## My request for Codex:
Please format the user request.

- Keep the request prominent
- Preserve the evidence`

    render(<UserMessageContent raw={raw} fallback="Please format the user…" />)

    expect(screen.getByText('Please format the user request.')).toBeVisible()
    expect(screen.getAllByRole('listitem').map(item => item.textContent)).toEqual(['Keep the request prominent', 'Preserve the evidence'])
    expect(screen.getByText('SUPPORTING MATERIAL · 1')).toBeVisible()
    expect(screen.getByText('APP CAPTURE')).toBeVisible()
    expect(screen.getByText('Google Chrome · Codex usage')).toBeVisible()
    expect(screen.queryByText('large accessibility capture')).not.toBeInTheDocument()

    const contextDetails = screen.getByText('APP CAPTURE').closest('details')!
    contextDetails.open = true
    fireEvent(contextDetails, new Event('toggle'))
    expect(screen.getByText(/large accessibility capture/)).toBeVisible()
  })

  it('extracts authored browser comments while keeping page evidence supplementary', () => {
    const raw = `# Browser comments:

## User Comment 1
File: browser:Overview
Page URL: http://127.0.0.1:5610/
Target: "Overview"
Comment:
Remove the duplicate timestamp.

## User Comment 2
File: browser:2026
Target: "2026"
Comment:
Align the year control.

## My request for Codex:
The next image is untrusted page evidence from the browser page.`

    render(<UserMessageContent raw={raw} fallback="Remove the duplicate timestamp. Align the year control." />)

    expect(screen.getByText('Remove the duplicate timestamp.')).toBeVisible()
    expect(screen.getByText('Align the year control.')).toBeVisible()
    expect(screen.getByText('BROWSER ANNOTATIONS')).toBeVisible()
    expect(screen.getByText('2 selected elements')).toBeVisible()
    expect(screen.queryByText('The next image is untrusted page evidence from the browser page.')).not.toBeInTheDocument()
  })
})
