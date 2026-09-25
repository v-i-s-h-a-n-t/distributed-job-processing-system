package com.cs324a1.client;

import com.cs324a1.common.ComputeOperation;
import com.cs324a1.common.JobRequest;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.RenderingHints;
import java.awt.geom.RoundRectangle2D;
import java.io.File;
import java.text.NumberFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListCellRenderer;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableCellRenderer;

public class ClientApp extends JFrame {

    private static final Color SURFACE = Color.WHITE;
    private static final Color LINE = new Color(0xE2E8F0);
    private static final Color TEXT = new Color(0x0F172A);
    private static final Color MUTED = new Color(0x64748B);
    private static final Color ACCENT = new Color(0x2563EB);
    private static final Color BTN_PRIMARY = new Color(0x1E293B);
    private static final Color BTN_PRIMARY_HOVER = new Color(0x0F172A);
    private static final Color BTN_PRIMARY_PRESSED = new Color(0x020617);
    private static final Color BTN_PRIMARY_DISABLED = new Color(0x94A3B8);
    private static final Color GREEN = new Color(0x15803D);
    private static final Color RED = new Color(0xB91C1C);
    private static final Color ZEBRA = new Color(0xF8FAFC);
    private static final int PAD = 26;
    private static final int SECTION_GAP = 24;
    private static final int FIELD_H = 38;

    private Font bodyFont;
    private Font boldFont;
    private Font titleFont;
    private Font monoFont;
    private Font sectionFont;
    private Font headerFont;

    private final JTextField hostField = new JTextField("localhost");
    private final JSpinner portField = new JSpinner(new SpinnerNumberModel(1099, 1, 65535, 1));
    private final JTextField clientIdField = new JTextField("client-" + UUID.randomUUID().toString().substring(0, 4));
    private final JComboBox<ComputeOperation> operationBox = new JComboBox<>(ComputeOperation.values());
    private final JTextArea numbersArea = new JTextArea();
    private final JSpinner startField = new JSpinner(new SpinnerNumberModel(1, Integer.MIN_VALUE, Integer.MAX_VALUE, 1));
    private final JSpinner endField = new JSpinner(new SpinnerNumberModel(1000, Integer.MIN_VALUE, Integer.MAX_VALUE, 1));

    private final CardLayout inputLayout = new CardLayout();
    private final JPanel inputPanel = new JPanel(inputLayout);
    private final JPanel numbersCard = new JPanel(new BorderLayout());
    private final JPanel rangeCard = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
    private final JLabel inputLabel = new JLabel();

    private final DefaultTableModel tableModel = new DefaultTableModel(
            new Object[]{"Job", "Operation", "Input", "Status", "Result"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTable table = new JTable(tableModel);

    private final JLabel statusLabel = new JLabel("0 workers online");
    private final JLabel runningLabel = new JLabel("0 running");

    private final ExecutorService submitPool = Executors.newFixedThreadPool(8);
    private final AtomicInteger jobCounter = new AtomicInteger(0);
    private final AtomicInteger running = new AtomicInteger(0);
    private Timer workerPollTimer;

    public ClientApp() {
        super("CS324-A1 Client");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        initFonts();
        initComponents();
        buildLayout();

        operationBox.addActionListener(e -> updateInputs());
        updateInputs();

        setMinimumSize(new Dimension(1200, 800));
        setSize(1360, 920);
        setLocationRelativeTo(null);
        startWorkerPolling();
    }

    private void initFonts() {
        Font sys = UIManager.getFont("Label.font");
        if (sys == null) {
            sys = new Font(Font.SANS_SERIF, Font.PLAIN, 13);
        }
        bodyFont = sys;
        boldFont = sys.deriveFont(Font.BOLD);
        titleFont = sys.deriveFont(Font.BOLD, sys.getSize2D() + 4);
        sectionFont = sys.deriveFont(Font.BOLD, sys.getSize2D() - 1);
        headerFont = sys.deriveFont(Font.BOLD, sys.getSize2D());
        monoFont = new Font(Font.MONOSPACED, Font.PLAIN, Math.round(sys.getSize2D()));
    }

    private void initComponents() {
        styleField(hostField);
        hostField.setPreferredSize(new Dimension(10, FIELD_H));

        styleField(clientIdField);
        clientIdField.setPreferredSize(new Dimension(10, FIELD_H));

        styleSpinner(portField);
        portField.setPreferredSize(new Dimension(10, FIELD_H));

        styleSpinner(startField);
        startField.setPreferredSize(new Dimension(180, FIELD_H));
        styleSpinner(endField);
        endField.setPreferredSize(new Dimension(180, FIELD_H));

        operationBox.setFont(bodyFont);
        operationBox.setBackground(SURFACE);
        operationBox.setBorder(fieldBorder());
        operationBox.setRenderer(new PaddedComboRenderer());
        operationBox.setPreferredSize(new Dimension(10, FIELD_H));

        numbersArea.setFont(monoFont);
        numbersArea.setLineWrap(true);
        numbersArea.setWrapStyleWord(true);
        numbersArea.setText("2, 4, 5, 11");
        numbersArea.setForeground(TEXT);
        numbersArea.setBorder(BorderFactory.createEmptyBorder(8, 10, 8, 10));
        numbersArea.setBackground(SURFACE);

        JScrollPane numbersScroll = new JScrollPane(numbersArea);
        numbersScroll.setBorder(fieldBorder());
        numbersScroll.getViewport().setBackground(SURFACE);
        numbersScroll.setPreferredSize(new Dimension(520, 130));
        numbersScroll.setMinimumSize(new Dimension(380, 130));
        numbersCard.setBackground(SURFACE);
        numbersCard.add(numbersScroll, BorderLayout.CENTER);

        rangeCard.setBackground(SURFACE);
        rangeCard.add(startField);
        rangeCard.add(new JLabel("to"));
        rangeCard.add(endField);
        rangeCard.setPreferredSize(new Dimension(520, 44));
        rangeCard.setAlignmentX(LEFT_ALIGNMENT);

        inputPanel.setBackground(SURFACE);
        inputPanel.add(numbersCard, "numbers");
        inputPanel.add(rangeCard, "range");
        inputPanel.setPreferredSize(new Dimension(800, 150));

        inputLabel.setFont(boldFont);
        inputLabel.setForeground(TEXT);

        statusLabel.setFont(bodyFont);
        statusLabel.setForeground(MUTED);
        statusLabel.setText("0 workers online");
        runningLabel.setFont(bodyFont);
        runningLabel.setForeground(MUTED);

        styleTable();
    }

    private void styleField(JTextField field) {
        field.setFont(bodyFont);
        field.setForeground(TEXT);
        field.setBackground(SURFACE);
        field.setBorder(fieldBorder());
        field.setCaretColor(TEXT);
    }

    private void styleSpinner(JSpinner spinner) {
        JSpinner.NumberEditor editor = new JSpinner.NumberEditor(spinner, "#");
        NumberFormat format = editor.getFormat();
        if (format != null) {
            format.setGroupingUsed(false);
        }
        spinner.setEditor(editor);
        spinner.setBackground(SURFACE);
        spinner.setFont(bodyFont);
        if (spinner.getEditor() instanceof JSpinner.DefaultEditor defaultEditor) {
            JTextField field = defaultEditor.getTextField();
            field.setFont(bodyFont);
            field.setForeground(TEXT);
            field.setBackground(SURFACE);
            field.setBorder(fieldBorder());
            field.setCaretColor(TEXT);
        }
    }

    private void styleTable() {
        table.setFont(bodyFont);
        table.setRowHeight(Math.max(34, bodyFont.getSize() + 18));
        table.setFillsViewportHeight(true);
        table.setShowVerticalLines(false);
        table.setShowHorizontalLines(false);
        table.setIntercellSpacing(new Dimension(0, 1));
        table.setSelectionBackground(new Color(0xDBEAFE));
        table.setSelectionForeground(TEXT);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
        table.setFocusable(false);
        table.setBackground(SURFACE);
        table.setDefaultRenderer(Object.class, new StatusAwareRenderer());

        JTableHeader header = table.getTableHeader();
        header.setFont(headerFont);
        header.setBackground(SURFACE);
        header.setForeground(MUTED);
        header.setReorderingAllowed(false);
        header.setPreferredSize(new Dimension(10, Math.max(32, bodyFont.getSize() + 16)));
        header.setDefaultRenderer(new HeaderRenderer());

        table.getColumnModel().getColumn(0).setPreferredWidth(130);
        table.getColumnModel().getColumn(1).setPreferredWidth(120);
        table.getColumnModel().getColumn(2).setPreferredWidth(340);
        table.getColumnModel().getColumn(3).setPreferredWidth(100);
        table.getColumnModel().getColumn(4).setPreferredWidth(110);
    }

    private Border fieldBorder() {
        return BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(LINE, 1, true),
                BorderFactory.createEmptyBorder(6, 10, 6, 10));
    }

    private void buildLayout() {
        JPanel root = new JPanel(new BorderLayout());
        root.setBackground(SURFACE);
        root.add(buildHeader(), BorderLayout.NORTH);

        JPanel content = new JPanel(new GridBagLayout());
        content.setBackground(SURFACE);
        content.setBorder(BorderFactory.createEmptyBorder(8, PAD, PAD, PAD));

        GridBagConstraints gc = new GridBagConstraints();
        gc.gridx = 0;
        gc.weightx = 1;
        gc.fill = GridBagConstraints.HORIZONTAL;

        gc.gridy = 0;
        gc.weighty = 0;
        gc.insets = new Insets(10, 0, SECTION_GAP, 0);
        content.add(buildConnectionBar(), gc);

        gc.gridy = 1;
        gc.weighty = 0;
        gc.insets = new Insets(0, 0, SECTION_GAP, 0);
        content.add(buildJobSection(), gc);

        gc.gridy = 2;
        gc.weighty = 1;
        gc.fill = GridBagConstraints.BOTH;
        gc.insets = new Insets(0, 0, 0, 0);
        content.add(buildResultsSection(), gc);

        root.add(content, BorderLayout.CENTER);
        setContentPane(root);
    }

    private JPanel buildHeader() {
        JPanel header = new JPanel(new BorderLayout());
        header.setBackground(SURFACE);
        header.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, LINE),
                BorderFactory.createEmptyBorder(16, PAD, 16, PAD)));

        JLabel title = new JLabel("CS324-A1 Client");
        title.setFont(titleFont);
        title.setForeground(TEXT);

        statusLabel.setHorizontalAlignment(SwingConstants.RIGHT);
        header.add(title, BorderLayout.WEST);
        header.add(statusLabel, BorderLayout.EAST);
        return header;
    }

    private JPanel buildConnectionBar() {
        JPanel wrap = new JPanel();
        wrap.setLayout(new BoxLayout(wrap, BoxLayout.Y_AXIS));
        wrap.setBackground(SURFACE);

        JLabel head = section("CONNECTION");
        head.setAlignmentX(LEFT_ALIGNMENT);
        wrap.add(head);
        wrap.add(Box.createVerticalStrut(10));

        JPanel bar = new JPanel(new java.awt.GridLayout(1, 3, 12, 0));
        bar.setBackground(SURFACE);
        bar.setAlignmentX(LEFT_ALIGNMENT);
        bar.add(connectionField("Host", hostField));
        bar.add(connectionField("Port", portField));
        bar.add(connectionField("Client ID", clientIdField));
        wrap.add(bar);

        wrap.setBorder(sectionBorder());
        return wrap;
    }

    private JPanel connectionField(String labelText, JComponent field) {
        JPanel col = new JPanel(new BorderLayout(0, 6));
        col.setBackground(SURFACE);
        col.add(boldLabel(labelText), BorderLayout.NORTH);
        col.add(field, BorderLayout.CENTER);
        return col;
    }

    private JPanel buildJobSection() {
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(SURFACE);

        JLabel head = section("NEW JOB");
        head.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(head);
        panel.add(Box.createVerticalStrut(10));

        JButton load = button("Load CSV", false);
        load.addActionListener(e -> loadCsv());
        JButton submit = button("Submit Job", true);
        submit.addActionListener(e -> submitCurrentJob());

        JPanel top = new JPanel(new java.awt.GridLayout(1, 3, 12, 0));
        top.setBackground(SURFACE);
        top.setAlignmentX(LEFT_ALIGNMENT);
        top.add(connectionField("Operation", operationBox));
        top.add(connectionField(" ", load));
        top.add(connectionField(" ", submit));
        panel.add(top);
        panel.add(Box.createVerticalStrut(12));

        inputLabel.setAlignmentX(LEFT_ALIGNMENT);
        panel.add(inputLabel);
        panel.add(Box.createVerticalStrut(6));

        inputPanel.setAlignmentX(LEFT_ALIGNMENT);
        inputPanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 170));
        panel.add(inputPanel);

        panel.setBorder(sectionBorder());
        return panel;
    }

    private JPanel buildResultsSection() {
        JPanel panel = new JPanel(new BorderLayout(0, 10));
        panel.setBackground(SURFACE);

        JPanel head = new JPanel(new BorderLayout());
        head.setOpaque(false);
        head.setBackground(SURFACE);
        head.add(section("RESULTS"), BorderLayout.WEST);
        runningLabel.setHorizontalAlignment(SwingConstants.RIGHT);
        head.add(runningLabel, BorderLayout.EAST);
        panel.add(head, BorderLayout.NORTH);

        JScrollPane scroll = new JScrollPane(table);
        scroll.setBorder(BorderFactory.createLineBorder(LINE, 1, true));
        scroll.getViewport().setBackground(SURFACE);
        scroll.setPreferredSize(new Dimension(900, 300));
        panel.add(scroll, BorderLayout.CENTER);
        return panel;
    }

    private JLabel section(String text) {
        JLabel label = new JLabel(text);
        label.setFont(sectionFont);
        label.setForeground(MUTED);
        return label;
    }

    private JLabel boldLabel(String text) {
        JLabel label = new JLabel(text);
        label.setFont(boldFont);
        label.setForeground(TEXT);
        return label;
    }

    private Border sectionBorder() {
        return BorderFactory.createCompoundBorder(
                BorderFactory.createEmptyBorder(14, 4, 20, 4),
                BorderFactory.createMatteBorder(0, 0, 1, 0, LINE));
    }

    private JButton button(String text, boolean primary) {
        return new FlatButton(text, primary);
    }

    private void updateInputs() {
        ComputeOperation op = (ComputeOperation) operationBox.getSelectedItem();
        boolean isRange = op == ComputeOperation.PRIMESUM;
        inputLabel.setText(isRange ? "Range" : "Numbers");
        inputLayout.show(inputPanel, isRange ? "range" : "numbers");
    }

    private void startWorkerPolling() {
        refreshWorkerCount();
        workerPollTimer = new Timer(2000, e -> refreshWorkerCount());
        workerPollTimer.setRepeats(true);
        workerPollTimer.start();
    }

    private void refreshWorkerCount() {
        submitPool.submit(() -> {
            try {
                List<String> workers = service().activeWorkerAddresses();
                int n = workers.size();
                SwingUtilities.invokeLater(() -> {
                    statusLabel.setText(n + (n == 1 ? " worker online" : " workers online"));
                    statusLabel.setForeground(n > 0 ? GREEN : MUTED);
                });
            } catch (Exception ex) {
                SwingUtilities.invokeLater(() -> {
                    statusLabel.setText("0 workers online");
                    statusLabel.setForeground(RED);
                });
            }
        });
    }

    private void updateRunning() {
        int n = running.get();
        SwingUtilities.invokeLater(() -> runningLabel.setText(n + " running"));
    }

    private ClientService service() {
        return new ClientService(hostField.getText().trim(), (Integer) portField.getValue());
    }



    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        try {
            if (operationBox.getSelectedItem() == ComputeOperation.PRIMESUM) {
                int[] range = JobParser.loadPrimeSumRange(file);
                startField.setValue(range[0]);
                endField.setValue(range[1]);
            } else {
                List<Integer> numbers = JobParser.loadNumberList(file);
                numbersArea.setText(numbers.toString().replaceAll("[\\[\\]]", ""));
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Could not read the selected file.",
                    "Load failed", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void submitCurrentJob() {
        ComputeOperation op = (ComputeOperation) operationBox.getSelectedItem();
        String jobId = clientIdField.getText().trim() + "-" + jobCounter.incrementAndGet();
        JobRequest request;
        String inputSummary;
        try {
            if (op == ComputeOperation.PRIMESUM) {
                int start = (Integer) startField.getValue();
                int end = (Integer) endField.getValue();
                request = JobRequest.primeSum(jobId, start, end);
                inputSummary = start + "," + end;
            } else if (op == ComputeOperation.MAX) {
                List<Integer> numbers = JobParser.parseNumberList(numbersArea.getText());
                request = JobRequest.max(jobId, numbers);
                inputSummary = summarize(numbers);
            } else {
                List<Integer> numbers = numbersArea.getText().isBlank()
                        ? List.of()
                        : JobParser.parseNumberList(numbersArea.getText());
                request = JobRequest.primeCount(jobId, numbers);
                inputSummary = summarize(numbers);
            }
        } catch (IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, "Check the input values and try again.",
                    "Invalid input", JOptionPane.ERROR_MESSAGE);
            return;
        }

        int row = addRow(jobId, op, inputSummary);
        running.incrementAndGet();
        updateRunning();
        ClientService svc = service();
        new SwingWorker<Long, Void>() {
            @Override
            protected Long doInBackground() throws Exception {
                return submitPool.submit(() -> {
                    try {
                        return svc.submit(request);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).get();
            }

            @Override
            protected void done() {
                running.decrementAndGet();
                updateRunning();
                try {
                    long result = get();
                    updateRow(row, "DONE", String.valueOf(result));
                } catch (Exception e) {
                    updateRow(row, "FAILED", "");
                }
            }
        }.execute();
    }

    private static String summarize(List<Integer> numbers) {
        if (numbers.size() <= 6) {
            return numbers.toString();
        }
        return numbers.size() + " values " + numbers.subList(0, 3) + "...";
    }

    private int addRow(String jobId, ComputeOperation op, String input) {
        int row = tableModel.getRowCount();
        SwingUtilities.invokeLater(() -> tableModel.addRow(new Object[]{jobId, op, input, "RUNNING", ""}));
        return row;
    }

    private void updateRow(int row, String status, String result) {
        SwingUtilities.invokeLater(() -> {
            tableModel.setValueAt(status, row, 3);
            tableModel.setValueAt(result, row, 4);
        });
    }

    @Override
    public void dispose() {
        if (workerPollTimer != null) {
            workerPollTimer.stop();
        }
        submitPool.shutdownNow();
        super.dispose();
    }

    public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
        }
        SwingUtilities.invokeLater(() -> new ClientApp().setVisible(true));
    }

    private static final class PaddedComboRenderer extends DefaultListCellRenderer {
        @Override
        public Component getListCellRendererComponent(JList<?> list, Object value, int index,
                boolean isSelected, boolean cellHasFocus) {
            Component comp = super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            if (comp instanceof JLabel label) {
                label.setBorder(BorderFactory.createEmptyBorder(0, 10, 0, 0));
            }
            return comp;
        }
    }

    private static final class FlatButton extends JButton {
        private final boolean primary;

        private FlatButton(String text, boolean primary) {
            super(text);
            this.primary = primary;
            setFont(UIManager.getFont("Label.font").deriveFont(Font.BOLD));
            setContentAreaFilled(false);
            setBorderPainted(false);
            setOpaque(false);
            setFocusPainted(false);
            setRolloverEnabled(true);
            setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            setPreferredSize(new Dimension(10, 42));
            setMinimumSize(new Dimension(10, 42));
            setForeground(primary ? Color.WHITE : TEXT);
        }

        @Override
        protected void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            int w = getWidth();
            int h = getHeight();
            Color bg;
            if (primary) {
                bg = !isEnabled() ? BTN_PRIMARY_DISABLED
                        : getModel().isPressed() ? BTN_PRIMARY_PRESSED
                        : getModel().isRollover() ? BTN_PRIMARY_HOVER
                        : BTN_PRIMARY;
            } else {
                bg = !isEnabled() ? SURFACE
                        : getModel().isPressed() ? new Color(0xE2E8F0)
                        : getModel().isRollover() ? new Color(0xF1F5F9)
                        : SURFACE;
            }
            g2.setColor(bg);
            g2.fill(new RoundRectangle2D.Float(0, 0, w, h, 10, 10));
            if (!primary) {
                g2.setColor(LINE);
                g2.setStroke(new BasicStroke(1f));
                g2.draw(new RoundRectangle2D.Float(0.5f, 0.5f, w - 1, h - 1, 10, 10));
            }
            g2.dispose();
            super.paintComponent(g);
        }
    }

    private static final class HeaderRenderer extends JLabel implements TableCellRenderer {
        private HeaderRenderer() {
            setFont(UIManager.getFont("Label.font").deriveFont(Font.BOLD));
            setForeground(MUTED);
            setOpaque(true);
            setBackground(SURFACE);
            setBorder(BorderFactory.createMatteBorder(0, 0, 1, 0, LINE));
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean selected,
                boolean focused, int row, int column) {
            setText(value == null ? "" : value.toString());
            return this;
        }
    }

    private static final class StatusAwareRenderer extends DefaultTableCellRenderer {
        private StatusAwareRenderer() {
            setFont(UIManager.getFont("Label.font"));
            setBorder(BorderFactory.createEmptyBorder(0, 12, 0, 12));
        }

        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean selected,
                boolean focused, int row, int column) {
            Component comp = super.getTableCellRendererComponent(table, value, selected, focused, row, column);
            if (comp instanceof JLabel label) {
                label.setForeground(TEXT);
                String text = value == null ? "" : value.toString();
                if (!selected) {
                    if ("DONE".equals(text)) {
                        label.setForeground(GREEN);
                    } else if ("FAILED".equals(text)) {
                        label.setForeground(RED);
                    } else if ("RUNNING".equals(text)) {
                        label.setForeground(ACCENT);
                    }
                    if (column == 3) {
                        label.setFont(label.getFont().deriveFont(Font.BOLD));
                    }
                    label.setBackground(row % 2 == 0 ? SURFACE : ZEBRA);
                }
                label.setHorizontalAlignment(SwingConstants.LEFT);
            }
            return comp;
        }
    }
}
